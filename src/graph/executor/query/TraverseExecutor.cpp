// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/TraverseExecutor.h"

#include "clients/storage/StorageClient.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

folly::Future<Status> TraverseExecutor::execute() {
  range_ = traverse_->stepRange();
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
  time::Duration dur;
  SCOPED_TIMER(&execTime_);
  auto inputVar = traverse_->inputVar();
  auto& inputResult = ectx_->getResult(inputVar);
  auto inputIter = inputResult.iter();
  auto iter = static_cast<SequentialIter*>(inputIter.get());

  reqDs_.colNames = {kVid};
  reqDs_.rows.reserve(iter->size());

  std::unordered_set<Value> uniqueSet;
  uniqueSet.reserve(iter->size());
  std::unordered_map<Value, Paths> prev;
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  const auto& vidType = *(spaceInfo.spaceDesc.vid_type_ref());
  auto* src = traverse_->src();
  QueryExpressionContext ctx(ectx_);

  bool mv = movable(traverse_->inputVars().front());
  for (; iter->valid(); iter->next()) {
    auto vid = src->eval(ctx(iter));
    if (!SchemaUtil::isValidVid(vid, vidType)) {
      LOG(ERROR) << "Mismatched vid type: " << vid.type()
                 << ", space vid type: " << SchemaUtil::typeToString(vidType);
      continue;
    }
    // Need copy here, Argument executor may depends on this variable.
    auto prePath = mv ? iter->moveRow() : *iter->row();
    buildPath(prev, vid, std::move(prePath));
    if (!uniqueSet.emplace(vid).second) {
      continue;
    }
    reqDs_.emplace_back(Row({std::move(vid)}));
  }
  paths_.emplace_back(std::move(prev));
  LOG(ERROR) << "buildRequestDataSet:" << dur.elapsedInUSec();
  return Status::OK();
}

folly::Future<Status> TraverseExecutor::traverse() {
  if (reqDs_.rows.empty()) {
    DataSet emptyResult;
    return finish(ResultBuilder().value(Value(std::move(emptyResult))).build());
  }
  return getNeighbors();
}

folly::Future<Status> TraverseExecutor::getNeighbors() {
  currentStep_++;
  time::Duration getNbrTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  bool finalStep = isFinalStep();
  StorageClient::CommonRequestParam param(traverse_->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return storageClient
      ->getNeighbors(param,
                     reqDs_.colNames,
                     std::move(reqDs_.rows),
                     traverse_->edgeTypes(),
                     traverse_->edgeDirection(),
                     finalStep ? traverse_->statProps() : nullptr,
                     traverse_->vertexProps(),
                     traverse_->edgeProps(),
                     finalStep ? traverse_->exprs() : nullptr,
                     finalStep ? traverse_->dedup() : false,
                     finalStep ? traverse_->random() : false,
                     finalStep ? traverse_->orderBy() : std::vector<storage::cpp2::OrderBy>(),
                     finalStep ? traverse_->limit(qctx()) : -1,
                     selectFilter())
      .via(runner())
      .thenValue([this, getNbrTime](StorageRpcResponse<GetNeighborsResponse>&& resp) mutable {
        SCOPED_TIMER(&execTime_);
        addStats(resp, getNbrTime.elapsedInUSec());
        return handleResponse(std::move(resp));
      });
}

Expression* TraverseExecutor::selectFilter() {
  Expression* filter = nullptr;
  if (!(currentStep_ == 1 && zeroStep())) {
    filter = const_cast<Expression*>(traverse_->filter());
  }
  if (currentStep_ == 1) {
    if (filter == nullptr) {
      filter = traverse_->firstStepFilter();
    } else if (traverse_->firstStepFilter() != nullptr) {
      filter = LogicalExpression::makeAnd(&objPool_, filter, traverse_->firstStepFilter());
    }
  }
  return filter;
}

void TraverseExecutor::addStats(RpcResponse& resp, int64_t getNbrTimeInUSec) {
  auto& hostLatency = resp.hostLatency();
  std::stringstream ss;
  ss << "{\n";
  for (size_t i = 0; i < hostLatency.size(); ++i) {
    size_t size = 0u;
    auto& result = resp.responses()[i];
    if (result.vertices_ref().has_value()) {
      size = (*result.vertices_ref()).size();
    }
    auto& info = hostLatency[i];
    ss << "{" << folly::sformat("{} exec/total/vertices: ", std::get<0>(info).toString())
       << folly::sformat("{}(us)/{}(us)/{},", std::get<1>(info), std::get<2>(info), size) << "\n"
       << folly::sformat("total_rpc_time: {}(us)", getNbrTimeInUSec) << "\n";
    auto detail = getStorageDetail(result.result.latency_detail_us_ref());
    if (!detail.empty()) {
      ss << folly::sformat("storage_detail: {}", detail);
    }
    ss << "\n}";
  }
  ss << "\n}";
  otherStats_.emplace(folly::sformat("step {}", currentStep_), ss.str());
}

folly::Future<Status> TraverseExecutor::handleResponse(RpcResponse&& resps) {
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
    VLOG(1) << "resp: " << *dataset;
    list.values.emplace_back(std::move(*dataset));
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  VLOG(1) << "storage response:" << *listVal;

  return std::move(buildInterimPath(std::make_unique<GetNeighborsIter>(listVal)))
      .via(runner())
      .thenValue([this](auto&& status) {
        if (!status.ok()) {
          return folly::makeFuture<Status>(std::move(status));
        }
        if (!isFinalStep()) {
          if (reqDs_.rows.empty()) {
            if (range_ != nullptr) {
              return folly::makeFuture<Status>(buildResult());
            } else {
              return folly::makeFuture<Status>(Status::OK());
            }
          } else {
            return getNeighbors();
          }
        } else {
          return folly::makeFuture<Status>(buildResult());
        }
      });
}

folly::Future<Status> TraverseExecutor::buildInterimPath(std::unique_ptr<GetNeighborsIter> iter) {
  size_t pathCnt = 0;
  const std::unordered_map<Value, Paths>& prev = paths_.back();
  if (currentStep_ == 1 && zeroStep()) {
    paths_.emplace_back();
    NG_RETURN_IF_ERROR(handleZeroStep(prev, iter->getVertices(), paths_.back(), pathCnt));
    // If 0..0 case, release memory and return immediately.
    if (range_ != nullptr && range_->max() == 0) {
      releasePrevPaths(pathCnt);
      return Status::OK();
    }
  }
  paths_.emplace_back();

  size_t totalSize = iter->size();
  size_t batchSize = getBatchSize(totalSize);

  // Start multiple jobs for handling the results
  std::vector<folly::Future<StatusOr<JobResult>>> futures;
  size_t begin = 0, end = 0, dispathedCnt = 0;
  while (dispathedCnt < totalSize) {
    end = begin + batchSize > totalSize ? totalSize : begin + batchSize;
    auto f =
        folly::makeFuture<Status>(Status::OK())
            .via(runner())
            .thenValue([this, begin, end, tmpIter = iter->copy(), prev](auto&& result) mutable {
              UNUSED(result);
              return handleJob(begin, end, tmpIter.get(), prev);
            });
    futures.emplace_back(std::move(f));
    VLOG(1) << "begin: " << begin << " end:" << end;
    begin = end;
    dispathedCnt += batchSize;
  }

  // Gather all results and do post works
  return folly::collect(futures).via(runner()).thenValue(
      [this, total = pathCnt](auto&& results) mutable {
        VLOG(1) << "path cnt: " << total;
        time::Duration dur;
        reqDs_.clear();
        std::unordered_map<Value, Paths>& current = paths_.back();
        size_t mapCnt = 0;
        for (auto& r : results) {
          if (!r.ok()) {
            return r.status();
          } else {
            mapCnt += r.value().newPaths.size();
          }
        }
        current.reserve(mapCnt);
        for (auto& r : results) {
          auto jobResult = std::move(r).value();
          total += jobResult.pathCnt;
          if (!jobResult.reqDs.rows.empty()) {
            reqDs_.rows.insert(reqDs_.rows.end(),
                               std::make_move_iterator(jobResult.reqDs.rows.begin()),
                               std::make_move_iterator(jobResult.reqDs.rows.end()));
          }
          for (auto& kv : jobResult.newPaths) {
            auto& paths = current[kv.first];
            paths.insert(paths.end(),
                         std::make_move_iterator(kv.second.begin()),
                         std::make_move_iterator(kv.second.end()));
          }
        }
        releasePrevPaths(total);
        LOG(ERROR) << "collect paths:" << dur.elapsedInUSec();
        return Status::OK();
      });
}

StatusOr<JobResult> TraverseExecutor::handleJob(size_t begin,
                                                size_t end,
                                                Iterator* iter,
                                                const std::unordered_map<Value, Paths>& prev) {
  time::Duration dur;
  // Iterates to the begin pos
  size_t tmp = 0;
  while (iter->valid() && ++tmp < begin) {
    iter->next();
  }

  // Handle edges from begin to end, [begin, end)
  JobResult jobResult;
  size_t& pathCnt = jobResult.pathCnt;
  DataSet& reqDs = jobResult.reqDs;
  reqDs.colNames = reqDs_.colNames;
  QueryExpressionContext ctx(ectx_);
  std::unordered_set<Value> uniqueDst;
  auto* vFilter = traverse_->vFilter() ? traverse_->vFilter()->clone() : nullptr;
  auto* eFilter = traverse_->eFilter() ? traverse_->eFilter()->clone() : nullptr;
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  std::unordered_map<Value, Paths>& current = jobResult.newPaths;
  for (; iter->valid() && tmp++ < end; iter->next()) {
    auto& dst = iter->getEdgeProp("*", kDst);
    if (!SchemaUtil::isValidVid(dst, *(spaceInfo.spaceDesc.vid_type_ref()))) {
      continue;
    }
    if (vFilter != nullptr && currentStep_ == 1) {
      auto& vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    if (eFilter != nullptr) {
      auto& eFilterVal = eFilter->eval(ctx(iter));
      if (!eFilterVal.isBool() || !eFilterVal.getBool()) {
        continue;
      }
    }
    auto srcV = iter->getVertex();
    auto e = iter->getEdge();
    // Join on dst = src
    auto pathToSrcFound = prev.find(srcV.getVertex().vid);
    if (pathToSrcFound == prev.end()) {
      return Status::Error("Can't find prev paths.");
    }
    const auto& paths = pathToSrcFound->second;
    for (auto& prevPath : paths) {
      if (hasSameEdge(prevPath, e.getEdge())) {
        continue;
      }
      if (uniqueDst.emplace(dst).second) {
        reqDs.rows.emplace_back(Row({std::move(dst)}));
      }
      if (currentStep_ == 1) {
        Row path;
        if (traverse_->trackPrevPath()) {
          path = prevPath;
        }
        path.values.emplace_back(srcV);
        List neighbors;
        neighbors.values.emplace_back(e);
        path.values.emplace_back(std::move(neighbors));
        VLOG(1) << "path: " << path;
        buildPath(current, dst, std::move(path));
        ++pathCnt;
      } else {
        auto path = prevPath;
        auto& eList = path.values.back().mutableList().values;
        eList.emplace_back(srcV);
        eList.emplace_back(e);
        VLOG(1) << "path: " << path;
        buildPath(current, dst, std::move(path));
        ++pathCnt;
      }
    }  // `prevPath'
  }    // `iter'
  LOG(ERROR) << "handleJob: " << dur.elapsedInUSec() << " begin:" << begin << " end:" << end;
  return jobResult;
}

void TraverseExecutor::buildPath(std::unordered_map<Dst, std::vector<Row>>& currentPaths,
                                 const Value& dst,
                                 Row&& path) {
  auto pathToDstFound = currentPaths.find(dst);
  if (pathToDstFound == currentPaths.end()) {
    Paths interimPaths;
    interimPaths.emplace_back(std::move(path));
    currentPaths.emplace(dst, std::move(interimPaths));
  } else {
    auto& interimPaths = pathToDstFound->second;
    interimPaths.emplace_back(std::move(path));
  }
}

Status TraverseExecutor::buildResult() {
  time::Duration dur;
  // This means we are reaching a dead end, return empty.
  if (range_ != nullptr && currentStep_ < range_->min()) {
    return finish(ResultBuilder().value(Value(DataSet())).build());
  }

  DataSet result;
  result.colNames = traverse_->colNames();
  result.rows.reserve(totalPathCnt_);
  for (auto& currentStepPaths : paths_) {
    for (auto& paths : currentStepPaths) {
      std::move(paths.second.begin(), paths.second.end(), std::back_inserter(result.rows));
    }
  }
  VLOG(1) << "result: " << result;
  LOG(ERROR) << "buildResult:" << dur.elapsedInUSec();

  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

bool TraverseExecutor::hasSameEdge(const Row& prevPath, const Edge& currentEdge) {
  for (const auto& v : prevPath.values) {
    if (v.isList()) {
      for (const auto& e : v.getList().values) {
        if (e.isEdge() && e.getEdge().keyEqual(currentEdge)) {
          return true;
        }
      }
    }
  }
  return false;
}

void TraverseExecutor::releasePrevPaths(size_t cnt) {
  if (range_ != nullptr) {
    if (currentStep_ == range_->min() && paths_.size() > 1) {
      auto rangeEnd = paths_.begin();
      std::advance(rangeEnd, paths_.size() - 1);
      paths_.erase(paths_.begin(), rangeEnd);
    } else if (range_->min() == 0 && currentStep_ == 1 && paths_.size() > 1) {
      paths_.pop_front();
    }

    if (currentStep_ >= range_->min()) {
      totalPathCnt_ += cnt;
    }
  } else {
    paths_.pop_front();
    totalPathCnt_ = cnt;
  }
}

Status TraverseExecutor::handleZeroStep(const std::unordered_map<Value, Paths>& prev,
                                        List&& vertices,
                                        std::unordered_map<Value, Paths>& zeroSteps,
                                        size_t& count) {
  std::unordered_set<Value> uniqueSrc;
  for (auto& srcV : vertices.values) {
    auto src = srcV.getVertex().vid;
    if (!uniqueSrc.emplace(src).second) {
      continue;
    }
    auto pathToSrcFound = prev.find(src);
    if (pathToSrcFound == prev.end()) {
      return Status::Error("Can't find prev paths.");
    }
    const auto& paths = pathToSrcFound->second;
    for (auto& p : paths) {
      Row path;
      if (traverse_->trackPrevPath()) {
        path = p;
      }
      path.values.emplace_back(srcV);
      List neighbors;
      neighbors.values.emplace_back(srcV);
      path.values.emplace_back(std::move(neighbors));
      buildPath(zeroSteps, src, std::move(path));
      ++count;
    }
  }
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
