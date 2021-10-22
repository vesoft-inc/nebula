/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/query/TraverseExecutor.h"

#include <sstream>

#include "clients/storage/GraphStorageClient.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"
#include "graph/context/QueryContext.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/ScopedTimer.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

folly::Future<Status> TraverseExecutor::execute() {
  steps_ = traverse_->steps();
  auto status = buildRequestDataSet();
  if (!status.ok()) {
    return error(std::move(status));
  }
  return traverse();
}

Status TraverseExecutor::close() {
  // clear the members
  reqDs_.rows.clear();
  return Executor::close();
}

Status TraverseExecutor::buildRequestDataSet() {
  SCOPED_TIMER(&execTime_);
  auto inputVar = traverse_->inputVar();
  VLOG(1) << node()->outputVar() << " : " << inputVar;
  auto& inputResult = ectx_->getResult(inputVar);
  auto iter = inputResult.iter();

  reqDs_ = buildRequestDataSetByVidType(iter.get(), traverse_->src(), traverse_->dedup());
  return Status::OK();
}

folly::Future<Status> TraverseExecutor::traverse() {
  if (reqDs_.rows.empty()) {
    VLOG(1) << "Empty input.";
    List emptyResult;
    return finish(ResultBuilder()
                      .value(Value(std::move(emptyResult)))
                      .iter(Iterator::Kind::kGetNeighbors)
                      .build());
  }
  getNeighbors();
  return promise_.getFuture();
}

void TraverseExecutor::getNeighbors() {
  currentStep_++;
  time::Duration getNbrTime;
  GraphStorageClient* storageClient = qctx_->getStorageClient();
  bool finalStep = isFinalStep();
  storageClient
      ->getNeighbors(traverse_->space(),
                     qctx()->rctx()->session()->id(),
                     qctx()->plan()->id(),
                     qctx()->plan()->isProfileEnabled(),
                     reqDs_.colNames,
                     std::move(reqDs_.rows),
                     traverse_->edgeTypes(),
                     traverse_->edgeDirection(),
                     finalStep ? traverse_->statProps() : nullptr,
                     finalStep ? traverse_->vertexProps() : nullptr,
                     finalStep ? traverse_->edgeProps() : traverse_->edgeDst(),
                     finalStep ? traverse_->exprs() : nullptr,
                     finalStep ? traverse_->dedup() : false,
                     finalStep ? traverse_->random() : false,
                     finalStep ? traverse_->orderBy() : std::vector<storage::cpp2::OrderBy>(),
                     finalStep ? traverse_->limit() : -1,
                     finalStep ? traverse_->filter() : nullptr)
      .via(runner())
      .ensure([this, getNbrTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc_time",
                            folly::stringPrintf("%lu(us)", getNbrTime.elapsedInUSec()));
      })
      .thenValue([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
        SCOPED_TIMER(&execTime_);
        addStats(resp);
        handleResponse(resp);
      });
}

void TraverseExecutor::addStats(RpcResponse& resp) {
  auto& hostLatency = resp.hostLatency();
  for (size_t i = 0; i < hostLatency.size(); ++i) {
    size_t size = 0u;
    auto& result = resp.responses()[i];
    if (result.vertices_ref().has_value()) {
      size = (*result.vertices_ref()).size();
    }
    auto& info = hostLatency[i];
    otherStats_.emplace(
        folly::sformat("{} exec/total/vertices", std::get<0>(info).toString()),
        folly::sformat("{}(us)/{}(us)/{},", std::get<1>(info), std::get<2>(info), size));
    auto detail = getStorageDetail(result.result.latency_detail_us_ref());
    if (!detail.empty()) {
      otherStats_.emplace("storage_detail", detail);
    }
  }
}

void TraverseExecutor::handleResponse(RpcResponse& resps) {
  SCOPED_TIMER(&execTime_);
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  if (!result.ok()) {
    promise_.setValue(std::move(result).status());
  }

  auto& responses = resps.responses();
  VLOG(1) << "Resp size: " << responses.size();
  List list;
  for (auto& resp : responses) {
    auto dataset = resp.get_vertices();
    if (dataset == nullptr) {
      LOG(INFO) << "Empty dataset in response";
      continue;
    }

    VLOG(1) << "Resp row size: " << dataset->rows.size() << "Resp : " << *dataset;
    list.values.emplace_back(std::move(*dataset));
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);
  VLOG(1) << "curr step: " << currentStep_ << " steps: " << steps_.nSteps();

  auto status = buildInterimPath(iter.get());
  if (!status.ok()) {
    promise_.setValue(status);
    return;
  }
  if (!isFinalStep()) {
    if (reqDs_.rows.empty()) {
      VLOG(1) << "Empty input.";
      if (steps_.isMToN()) {
        promise_.setValue(buildResult());
      } else {
        promise_.setValue(Status::OK());
      }
    } else {
      getNeighbors();
    }
  } else {
    promise_.setValue(buildResult());
  }
}

Status TraverseExecutor::buildInterimPath(GetNeighborsIter* iter) {
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  DataSet reqDs;
  reqDs.colNames = reqDs_.colNames;
  std::unordered_set<Value> uniqueVid;

  const std::unordered_map<Value, Paths>* prev = nullptr;
  std::unordered_map<Value, Paths>* current = nullptr;
  if (currentStep_ == 0) {
    paths_.emplace_back();
    current = &paths_.back();
  } else {
    prev = &paths_.back();
    paths_.emplace_back();
    current = &paths_.back();
  }

  for (; iter->valid(); iter->next()) {
    auto& dst = iter->getEdgeProp("*", kDst);
    if (!SchemaUtil::isValidVid(dst, *(spaceInfo.spaceDesc.vid_type_ref()))) {
      continue;
    }
    if (uniqueVid.emplace(dst).second) {
      reqDs.rows.emplace_back(Row({std::move(dst)}));
    }

    auto srcV = iter->getVertex();
    auto e = iter->getEdge();
    // If we don't find one, it means the first step
    if (prev == nullptr) {
      Row row;
      row.values.emplace_back(std::move(srcV));
      List neighbors;
      neighbors.values.emplace_back(std::move(e));
      row.values.emplace_back(std::move(neighbors));
      std::vector<Row> rows;
      rows.emplace_back(std::move(row));
      current->emplace(dst, std::move(rows));
    } else {
      // Join on dst = src
      auto pathToSrcFound = prev->find(srcV.getVertex().vid);
      if (pathToSrcFound == prev->end()) {
        return Status::Error("Can't find prev paths.");
      }
      const auto& paths = pathToSrcFound->second;
      for (auto path : paths) {
        path.values[1].mutableList().values.emplace_back(std::move(srcV));
        path.values[1].mutableList().values.emplace_back(std::move(e));
        auto pathToDstFound = current->find(dst);
        if (pathToDstFound == current->end()) {
          std::vector<Row> rows;
          rows.emplace_back(std::move(path));
          current->emplace(dst, std::move(rows));
        } else {
          auto& rows = pathToDstFound->second;
          rows.emplace_back(std::move(path));
        }
      }
    }
  }

  if (steps_.isMToN() && currentStep_ < steps_.mSteps() && paths_.size() > 1) {
    paths_.pop_front();
  } else if (paths_.size() > 1) {
    paths_.pop_front();
  }
  return Status::OK();
}

Status TraverseExecutor::buildResult() {
  auto inputVar = traverse_->inputVar();
  VLOG(1) << node()->outputVar() << " : " << inputVar;
  auto& inputResult = ectx_->getResult(inputVar);
  auto iter = inputResult.iter();

  std::unordered_map<Value, std::vector<const Row*>> hashTable;
  auto hashKey = traverse_->src();
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    Value key = hashKey->eval(ctx(iter.get()));
    auto& vals = hashTable[key];
    vals.emplace_back(iter->row());
  }

  DataSet result;
  for (auto& currentStepPaths : paths_) {
    for (auto& paths : currentStepPaths) {
      for (auto& path : paths.second) {
        auto prevPathFound = hashTable.find(path.values[0].getVertex().vid);
        if (prevPathFound == hashTable.end()) {
          return Status::Error("Can't find prev paths.");
        }
        auto& prevPaths = prevPathFound->second;
        for (const auto* prevPath : prevPaths) {
          auto newPath = *prevPath;
          newPath.values.insert(newPath.values.end(), path.values.begin(), path.values.end());
          result.emplace_back(std::move(newPath));
        }
      }
    }
  }
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}
}  // namespace graph
}  // namespace nebula
