// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/SubgraphExecutor.h"

#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {

folly::Future<Status> SubgraphExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  startVids_ = buildRequestDataSet();
  if (startVids_.rows.empty()) {
    DataSet emptyResult;
    return finish(ResultBuilder().value(Value(std::move(emptyResult))).build());
  }
  return getNeighbors();
}

DataSet SubgraphExecutor::buildRequestDataSet() {
  auto inputVar = subgraph_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iter();
  auto iter = static_cast<SequentialIter*>(inputIter.get());
  return buildRequestDataSetByVidType(iter.get(), subgraph_->src(), true);
}

folly::Future<Status> SubgraphExecutor::getNeighbors() {
  time::Duration getNbrTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(subgraph_->space(),
                                          qctx_->rctx()->session()->id(),
                                          qctx_->plan()->id(),
                                          qctx_->plan()->isProfileEnabled());

  storage::cpp2::EdgeDirection edgeDirection{Direction::OUT_EDGE};
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(startVids_.rows),
                     {},
                     edgeDirection,
                     nullptr,
                     subgraph_->vertexProps(),
                     subgraph_->edgeProps(),
                     nullptr,
                     false,
                     false,
                     {},
                     -1,
                     currentStep_ == 1 ? subgraph_->edgeFilter() : subgraph_->filter())
      .via(runner())
      .thenValue([this, getNbrTime](StorageRpcResponse<GetNeighborsResponse>&& resp) mutable {
        // addStats(resp, getNbrTime.elapsedInUSec());
        return handleResponse(std::move(resp));
      });
}

folly::Future<Status> SubgraphExecutor::handleResponse(RpcResponse&& resps) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  if (!result.ok()) {
    return folly::makeFuture<Status>(std::move(result).status());
  }

  auto& responses = resps.responses();
  List list;
  for (auto& resp : responses) {
    auto dataset = resp.get_vertices();
    if (dataset == nullptr) {
      continue;
    }
    list.values.emplace_back(std::move(*dataset));
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);

  if (!process(iter.get()) || ++currentStep_ > totalSteps_) {
    return folly::makeFuture<Status>(Status::OK());
  } else {
    return getNeighbors();
  }
}

bool SubgraphExecutor::process(GetNeighborsIter* iter) {
  auto& rows = startVids_.rows;
  auto gnSize = iter->size();
  if (gnSize == 0) {
    return false;
  }

  ResultBuilder builder;
  builder.value(iter->valuePtr());

  robin_hood::unordered_flat_map<Value, int64_t, std::hash<Value>> currentVids;
  currentVids.reserve(gnSize);
  historyVids_.reserve(historyVids_.size() + gnSize);
  if (currentStep_ == 1) {
    for (; iter->valid(); iter->next()) {
      const auto& src = iter->getColumn(0);
      currentVids.emplace(src, 0);
    }
    iter->reset();
  }
  auto& biDirectEdgeTypes = subgraph_->biDirectEdgeTypes();
  while (iter->valid()) {
    const auto& dst = iter->getEdgeProp("*", nebula::kDst);
    auto findIter = historyVids_.find(dst);
    if (findIter != historyVids_.end()) {
      if (biDirectEdgeTypes.empty()) {
        iter->next();
      } else {
        const auto& typeVal = iter->getEdgeProp("*", nebula::kType);
        if (UNLIKELY(!typeVal.isInt())) {
          iter->erase();
          continue;
        }
        auto type = typeVal.getInt();
        if (biDirectEdgeTypes.find(type) != biDirectEdgeTypes.end()) {
          if (type < 0 || findIter->second + 2 == currentStep_) {
            iter->erase();
          } else {
            iter->next();
          }
        } else {
          iter->next();
        }
      }
    } else {
      if (currentStep_ == totalSteps_) {
        iter->erase();
        continue;
      }
      if (currentVids.emplace(dst, currentStep_).second) {
        // next vids for getNeighbor
        Row row;
        row.values.emplace_back(std::move(dst));
        rows.emplace_back(std::move(row));
      }
      iter->next();
    }
  }
  if (rows.empty()) {
    return false;
  }

  iter->reset();
  builder.iter(std::move(iter));
  finish(builder.build());
  // update historyVids
  historyVids_.insert(std::make_move_iterator(currentVids.begin()),
                      std::make_move_iterator(currentVids.end()));
  return true;
}

}  // namespace graph
}  // namespace nebula
