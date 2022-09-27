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
  auto status = buildRequestVids();
  if (!status.ok()) {
    return error(std::move(status));
  }
  return traverse();
}

Status TraverseExecutor::close() {
  // clear the members
  vids_.clear();
  return Executor::close();
}

Status TraverseExecutor::buildRequestVids() {
  time::Duration dur;
  SCOPED_TIMER(&execTime_);
  const auto& inputVar = traverse_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iterRef();
  auto iter = static_cast<SequentialIter*>(inputIter);

  const auto& spaceInfo = qctx()->rctx()->session()->space();
  const auto& metaVidType = *(spaceInfo.spaceDesc.vid_type_ref());
  auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
  auto* src = traverse_->src();

  QueryExpressionContext ctx(ectx_);
  HashMap prev;

  bool mv = movable(traverse_->inputVars().front());
  for (; iter->valid(); iter->next()) {
    const auto& vid = src->eval(ctx(iter));
    if (vid.type() != vidType) {
      LOG(ERROR) << "Mismatched vid type: " << vid.type() << ", space vid type: " << vidType;
      continue;
    }
    // Need copy here, Argument executor may depends on this variable.
    auto prePath = mv ? iter->moveRow() : *iter->row();
    buildPath(prev, vid, std::move(prePath));
    vids_.emplace(vid);
  }
  paths_.emplace_back(std::move(prev));
  return Status::OK();
}

folly::Future<Status> TraverseExecutor::traverse() {
  if (vids_.empty()) {
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
  std::vector<Value> vids(vids_.begin(), vids_.end());
  vids_.clear();
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
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
                     selectFilter(),
                     nullptr)
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
    list.values.emplace_back(std::move(*dataset));
  }
  auto listVal = std::make_shared<Value>(std::move(list));

  if (FLAGS_max_job_size <= 1) {
    auto iter = std::make_unique<GetNeighborsIter>(listVal);
    auto status = buildInterimPath(iter.get());
    if (!status.ok()) {
      return folly::makeFuture<Status>(std::move(status));
    }
    if (!isFinalStep()) {
      if (vids_.empty()) {
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
  } else {
    return std::move(buildInterimPathMultiJobs(std::make_unique<GetNeighborsIter>(listVal)))
        .via(runner())
        .thenValue([this](auto&& status) {
          if (!status.ok()) {
            return folly::makeFuture<Status>(std::move(status));
          }
          if (!isFinalStep()) {
            if (vids_.empty()) {
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
}

Status TraverseExecutor::buildInterimPath(GetNeighborsIter* iter) {
  size_t count = 0;

  const auto& prev = paths_.back();
  if (currentStep_ == 1 && zeroStep()) {
    paths_.emplace_back();
    NG_RETURN_IF_ERROR(handleZeroStep(prev, iter->getVertices(), paths_.back(), count));
    // If 0..0 case, release memory and return immediately.
    if (range_ != nullptr && range_->max() == 0) {
      releasePrevPaths(count);
      return Status::OK();
    }
  }
  paths_.emplace_back();
  auto& current = paths_.back();

  auto* vFilter = traverse_->vFilter();
  auto* eFilter = traverse_->eFilter();
  QueryExpressionContext ctx(ectx_);

  for (; iter->valid(); iter->next()) {
    if (vFilter != nullptr && currentStep_ == 1) {
      const auto& vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    if (eFilter != nullptr) {
      const auto& eFilterVal = eFilter->eval(ctx(iter));
      if (!eFilterVal.isBool() || !eFilterVal.getBool()) {
        continue;
      }
    }
    auto& dst = iter->getEdgeProp("*", kDst);
    if (dst.type() == Value::Type::__EMPTY__) {
      // no edge return Empty
      continue;
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
      vids_.emplace(dst);

      if (currentStep_ == 1) {
        Row path;
        if (traverse_->trackPrevPath()) {
          path = prevPath;
        }
        path.values.emplace_back(srcV);
        List neighbors;
        neighbors.values.emplace_back(e);
        path.values.emplace_back(std::move(neighbors));
        buildPath(current, dst, std::move(path));
        ++count;
      } else {
        auto path = prevPath;
        auto& eList = path.values.back().mutableList().values;
        eList.emplace_back(srcV);
        eList.emplace_back(e);
        buildPath(current, dst, std::move(path));
        ++count;
      }
    }  // `prevPath'
  }    // `iter'

  releasePrevPaths(count);
  return Status::OK();
}

folly::Future<Status> TraverseExecutor::buildInterimPathMultiJobs(
    std::unique_ptr<GetNeighborsIter> iter) {
  size_t pathCnt = 0;
  const auto* prev = &paths_.back();
  if (currentStep_ == 1 && zeroStep()) {
    paths_.emplace_back();
    NG_RETURN_IF_ERROR(handleZeroStep(*prev, iter->getVertices(), paths_.back(), pathCnt));
    // If 0..0 case, release memory and return immediately.
    if (range_ != nullptr && range_->max() == 0) {
      releasePrevPaths(pathCnt);
      return Status::OK();
    }
  }
  paths_.emplace_back();

  auto scatter = [this, prev](
                     size_t begin, size_t end, Iterator* tmpIter) mutable -> StatusOr<JobResult> {
    return handleJob(begin, end, tmpIter, *prev);
  };

  auto gather = [this, pathCnt](std::vector<StatusOr<JobResult>> results) mutable -> Status {
    auto& current = paths_.back();
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
      pathCnt += jobResult.pathCnt;
      auto& vids = jobResult.vids;
      if (!vids.empty()) {
        vids_.insert(std::make_move_iterator(vids.begin()), std::make_move_iterator(vids.end()));
      }
      for (auto& kv : jobResult.newPaths) {
        auto& paths = current[kv.first];
        paths.insert(paths.end(),
                     std::make_move_iterator(kv.second.begin()),
                     std::make_move_iterator(kv.second.end()));
      }
    }
    releasePrevPaths(pathCnt);
    return Status::OK();
  };

  return runMultiJobs(std::move(scatter), std::move(gather), iter.get());
}

StatusOr<JobResult> TraverseExecutor::handleJob(size_t begin,
                                                size_t end,
                                                Iterator* iter,
                                                const HashMap& prev) {
  // Handle edges from begin to end, [begin, end)
  JobResult jobResult;
  size_t& pathCnt = jobResult.pathCnt;
  auto& vids = jobResult.vids;
  QueryExpressionContext ctx(ectx_);
  auto* vFilter = traverse_->vFilter() ? traverse_->vFilter()->clone() : nullptr;
  auto* eFilter = traverse_->eFilter() ? traverse_->eFilter()->clone() : nullptr;
  auto& current = jobResult.newPaths;
  for (; iter->valid() && begin++ < end; iter->next()) {
    if (vFilter != nullptr && currentStep_ == 1) {
      const auto& vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    if (eFilter != nullptr) {
      const auto& eFilterVal = eFilter->eval(ctx(iter));
      if (!eFilterVal.isBool() || !eFilterVal.getBool()) {
        continue;
      }
    }
    auto& dst = iter->getEdgeProp("*", kDst);
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
      vids.emplace(dst);
      if (currentStep_ == 1) {
        Row path;
        if (traverse_->trackPrevPath()) {
          path = prevPath;
        }
        path.values.emplace_back(srcV);
        List neighbors;
        neighbors.values.emplace_back(e);
        path.values.emplace_back(std::move(neighbors));
        buildPath(current, dst, std::move(path));
        ++pathCnt;
      } else {
        auto path = prevPath;
        auto& eList = path.values.back().mutableList().values;
        eList.emplace_back(srcV);
        eList.emplace_back(e);
        buildPath(current, dst, std::move(path));
        ++pathCnt;
      }
    }  // `prevPath'
  }    // `iter'
  return jobResult;
}

void TraverseExecutor::buildPath(HashMap& currentPaths, const Value& dst, Row&& path) {
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
  time::Duration dur;
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

Status TraverseExecutor::handleZeroStep(const HashMap& prev,
                                        List&& vertices,
                                        HashMap& zeroSteps,
                                        size_t& count) {
  HashSet uniqueSrc;
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
