// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/ExpandAllExecutor.h"

#include "common/algorithm/ReservoirSampling.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

Status ExpandAllExecutor::buildRequestVids() {
  SCOPED_TIMER(&execTime_);
  const auto& inputVar = expand_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iterRef();
  auto iter = static_cast<SequentialIter*>(inputIter);
  size_t iterSize = iter->size();
  nextStepVids_.reserve(iterSize);
  if (joinInput_) {
    for (; iter->valid(); iter->next()) {
      const auto& src = iter->getColumn(0);
      const auto& dst = iter->getColumn(1);
      nextStepVids_.emplace(dst);
      // update preDst2vidMap_
      auto findDst = preDst2VidsMap_.find(dst);
      if (findDst == preDst2VidsMap_.end()) {
        std::unordered_set<Value> tmp({src});
        preDst2VidsMap_.emplace(dst, std::move(tmp));
      } else {
        findDst->second.emplace(src);
      }
    }
  } else {
    for (; iter->valid(); iter->next()) {
      const auto& dst = iter->getColumn(-1);
      nextStepVids_.emplace(dst);
    }
  }
  return Status::OK();
}

folly::Future<Status> ExpandAllExecutor::execute() {
  currentStep_ = expand_->minSteps();
  maxSteps_ = expand_->maxSteps();
  vertexColumns_ = expand_->vertexColumns();
  edgeColumns_ = expand_->edgeColumns();
  sample_ = expand_->sample();
  stepLimits_ = expand_->stepLimits();
  joinInput_ = expand_->joinInput();
  result_.colNames = expand_->colNames();

  NG_RETURN_IF_ERROR(buildRequestVids());
  if (nextStepVids_.empty()) {
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  }
  return getNeighbors();
}

folly::Future<Status> ExpandAllExecutor::getNeighbors() {
  currentStep_++;
  StorageClient* storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(expand_->space(),
                                          qctx_->rctx()->session()->id(),
                                          qctx_->plan()->id(),
                                          qctx_->plan()->isProfileEnabled());
  std::vector<Value> vids(nextStepVids_.size());
  std::move(nextStepVids_.begin(), nextStepVids_.end(), vids.begin());
  QueryExpressionContext qec(qctx()->ectx());
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     {},
                     storage::cpp2::EdgeDirection::OUT_EDGE,
                     nullptr,
                     expand_->vertexProps(),
                     expand_->edgeProps(),
                     nullptr,
                     false,
                     false,
                     std::vector<storage::cpp2::OrderBy>(),
                     expand_->limit(qec),
                     expand_->filter(),
                     nullptr)
      .via(runner())
      .thenValue([this](RpcResponse&& resp) mutable {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        nextStepVids_.clear();
        SCOPED_TIMER(&execTime_);
        addStats(resp);
        time::Duration expandTime;
        curLimit_ = 0;
        curMaxLimit_ = stepLimits_.empty() ? std::numeric_limits<int64_t>::max()
                                           : stepLimits_[currentStep_ - 2];
        return handleResponse(std::move(resp)).ensure([this, expandTime]() {
          std::string timeName = "graphExpandAllTime+" + folly::to<std::string>(currentStep_);
          otherStats_.emplace(timeName, folly::sformat("{}(us)", expandTime.elapsedInUSec()));
        });
      })
      .thenValue([this](Status s) -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(s);
        if (qctx()->isKilled()) {
          return Status::OK();
        }
        if (currentStep_ <= maxSteps_) {
          if (!nextStepVids_.empty()) {
            return getNeighbors();
          }
          if (!preVisitedVids_.empty()) {
            return expandFromCache();
          }
        }
        return finish(ResultBuilder().value(Value(std::move(result_))).build());
      });
}

folly::Future<Status> ExpandAllExecutor::expandFromCache() {
  for (; currentStep_ <= maxSteps_; ++currentStep_) {
    time::Duration expandTime;
    if (qctx()->isKilled()) {
      return Status::OK();
    }
    curLimit_ = 0;
    curMaxLimit_ =
        stepLimits_.empty() ? std::numeric_limits<int64_t>::max() : stepLimits_[currentStep_ - 2];

    std::vector<int64_t> samples;
    if (sample_) {
      int64_t size = 0;
      for (auto& vid : preVisitedVids_) {
        size += adjList_[vid].size();
      }
      algorithm::ReservoirSampling<int64_t> sampler(curMaxLimit_, size);
      samples = sampler.samples();
      std::sort(samples.begin(), samples.end(), [](int64_t a, int64_t b) { return a > b; });
    }

    std::unordered_map<Value, std::unordered_set<Value>> dst2VidsMap;
    std::unordered_set<Value> visitedVids;
    getNeighborsFromCache(dst2VidsMap, visitedVids, samples);
    preVisitedVids_.swap(visitedVids);
    preDst2VidsMap_.swap(dst2VidsMap);
    std::string timeName = "graphCacheExpandAllTime+" + folly::to<std::string>(currentStep_);
    otherStats_.emplace(timeName, folly::sformat("{}(us)", expandTime.elapsedInUSec()));
    if (!nextStepVids_.empty()) {
      return getNeighbors();
    }
  }
  return finish(ResultBuilder().value(Value(std::move(result_))).build());
}

void ExpandAllExecutor::getNeighborsFromCache(
    std::unordered_map<Value, std::unordered_set<Value>>& dst2VidsMap,
    std::unordered_set<Value>& visitedVids,
    std::vector<int64_t>& samples) {
  for (const auto& vid : preVisitedVids_) {
    auto findVid = adjList_.find(vid);
    if (findVid == adjList_.end()) {
      continue;
    }
    auto& adjEdgeProps = findVid->second;
    auto& vertexProps = adjEdgeProps.back();
    for (auto edgeIter = adjEdgeProps.begin(); edgeIter != adjEdgeProps.end() - 1; ++edgeIter) {
      if (sample_) {
        if (samples.empty()) {
          break;
        }
        if (curLimit_++ != samples.back()) {
          continue;
        } else {
          samples.pop_back();
        }
      } else {
        if (curLimit_++ >= curMaxLimit_) {
          break;
        }
      }

      auto& dst = (*edgeIter).values.back();
      if (adjList_.find(dst) == adjList_.end()) {
        nextStepVids_.emplace(dst);
      } else {
        visitedVids.emplace(dst);
      }
      if (joinInput_) {
        auto findInitVid = preDst2VidsMap_.find(vid);
        buildResult(findInitVid->second, vertexProps, *edgeIter);
        updateDst2VidsMap(dst2VidsMap, vid, dst);
      } else {
        buildResult(vertexProps, *edgeIter);
      }
    }
  }
  resetNextStepVids(visitedVids);
}

folly::Future<Status> ExpandAllExecutor::handleResponse(RpcResponse&& resps) {
  NG_RETURN_IF_ERROR(handleCompleteness(resps, FLAGS_accept_partial_success));
  List list;
  for (auto& resp : resps.responses()) {
    auto dataset = resp.get_vertices();
    if (dataset) {
      list.values.emplace_back(std::move(*dataset));
    }
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);
  if (iter->numRows() == 0) {
    return Status::OK();
  }
  auto size = iter->size();
  std::vector<int64_t> samples;
  if (sample_) {
    algorithm::ReservoirSampling<int64_t> sampler(curMaxLimit_, size);
    samples = sampler.samples();
    std::sort(samples.begin(), samples.end(), [](int64_t a, int64_t b) { return a > b; });
  }

  QueryExpressionContext ctx(ectx_);
  std::unordered_map<Value, std::unordered_set<Value>> dst2VidsMap;
  std::unordered_set<Value> visitedVids;
  std::vector<List> adjEdgeProps;
  List curVertexProps;
  Value curVid;
  if (iter->valid() && vertexColumns_) {
    for (auto& col : vertexColumns_->columns()) {
      Value val = col->expr()->eval(ctx(iter.get()));
      curVertexProps.values.emplace_back(std::move(val));
    }
  }
  for (; iter->valid(); iter->next()) {
    List edgeProps;
    if (edgeColumns_) {
      for (auto& col : edgeColumns_->columns()) {
        Value val = col->expr()->eval(ctx(iter.get()));
        edgeProps.values.emplace_back(std::move(val));
      }
    }
    // get the start vid of the next step through dst when expanding in the cache
    auto dst = iter->getEdgeProp("*", nebula::kDst);
    if (dst.isNull()) {
      // (TEST)
      continue;
    }
    edgeProps.values.emplace_back(dst);
    const auto& vid = iter->getColumn(0);
    curVid = curVid.empty() ? vid : curVid;
    if (curVid != vid) {
      adjEdgeProps.emplace_back(std::move(curVertexProps));
      adjList_.emplace(curVid, std::move(adjEdgeProps));
      curVid = vid;
      if (vertexColumns_) {
        for (auto& col : vertexColumns_->columns()) {
          Value val = col->expr()->eval(ctx(iter.get()));
          curVertexProps.values.emplace_back(std::move(val));
        }
      }
    }
    adjEdgeProps.emplace_back(edgeProps);

    if (sample_) {
      if (samples.empty()) {
        continue;
      }
      if (curLimit_++ != samples.back()) {
        continue;
      } else {
        samples.pop_back();
      }
    } else {
      if (curLimit_++ >= curMaxLimit_) {
        continue;
      }
    }

    if (joinInput_) {
      auto findVid = preDst2VidsMap_.find(vid);
      buildResult(findVid->second, curVertexProps, edgeProps);
      updateDst2VidsMap(dst2VidsMap, vid, dst);
    } else {
      buildResult(curVertexProps, edgeProps);
    }

    if (adjList_.find(dst) == adjList_.end()) {
      nextStepVids_.emplace(dst);
    } else {
      visitedVids.emplace(dst);
    }
  }
  if (!curVid.empty()) {
    adjEdgeProps.emplace_back(std::move(curVertexProps));
    adjList_.emplace(curVid, std::move(adjEdgeProps));
  }

  resetNextStepVids(visitedVids);

  if (!preVisitedVids_.empty()) {
    getNeighborsFromCache(dst2VidsMap, visitedVids, samples);
  }
  visitedVids.swap(preVisitedVids_);
  dst2VidsMap.swap(preDst2VidsMap_);
  return Status::OK();
}

void ExpandAllExecutor::buildResult(const std::unordered_set<Value>& vids,
                                    const List& vList,
                                    const List& eList) {
  if (vList.values.empty() && eList.values.empty()) {
    return;
  }
  for (auto& vid : vids) {
    Row row;
    row.values.emplace_back(vid);
    row.values.insert(row.values.end(), vList.values.begin(), vList.values.end());
    row.values.insert(row.values.end(), eList.values.begin(), eList.values.end() - 1);
    result_.rows.emplace_back(std::move(row));
  }
}

void ExpandAllExecutor::buildResult(const List& vList, const List& eList) {
  if (vList.values.empty() && eList.values.empty()) {
    return;
  }
  Row row = vList;
  row.values.insert(row.values.end(), eList.values.begin(), eList.values.end() - 1);
  result_.rows.emplace_back(std::move(row));
}

void ExpandAllExecutor::resetNextStepVids(std::unordered_set<Value>& visitedVids) {
  auto vidIter = nextStepVids_.begin();
  while (vidIter != nextStepVids_.end()) {
    if (adjList_.find(*vidIter) != adjList_.end()) {
      visitedVids.emplace(*vidIter);
      vidIter = nextStepVids_.erase(vidIter);
    } else {
      vidIter++;
    }
  }
}

void ExpandAllExecutor::updateDst2VidsMap(
    std::unordered_map<Value, std::unordered_set<Value>>& dst2VidsMap,
    const Value& src,
    const Value& dst) {
  auto findSrc = preDst2VidsMap_.find(src);
  if (findSrc == preDst2VidsMap_.end()) {
    auto findDst = dst2VidsMap.find(dst);
    if (findDst == dst2VidsMap.end()) {
      std::unordered_set<Value> tmp({src});
      dst2VidsMap.emplace(dst, std::move(tmp));
    } else {
      findDst->second.emplace(src);
    }
  } else {
    auto findDst = dst2VidsMap.find(dst);
    if (findDst == dst2VidsMap.end()) {
      dst2VidsMap.emplace(dst, findSrc->second);
    } else {
      findDst->second.insert(findSrc->second.begin(), findSrc->second.end());
    }
  }
}

}  // namespace graph
}  // namespace nebula
