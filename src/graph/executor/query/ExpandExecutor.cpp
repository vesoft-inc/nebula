// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/ExpandExecutor.h"

#include "common/algorithm/ReservoirSampling.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetDstBySrcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

Status ExpandExecutor::buildRequestVids() {
  SCOPED_TIMER(&execTime_);
  const auto& inputVar = expand_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iter();
  auto iter = static_cast<SequentialIter*>(inputIter.get());
  size_t iterSize = iter->size();
  nextStepVids_.reserve(iterSize);
  QueryExpressionContext ctx(ectx_);

  const auto& spaceInfo = qctx()->rctx()->session()->space();
  const auto& metaVidType = *(spaceInfo.spaceDesc.vid_type_ref());
  auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
  for (; iter->valid(); iter->next()) {
    const auto& vid = iter->getColumn(0);
    if (vid.type() == vidType) {
      nextStepVids_.emplace(vid);
    }
  }
  return Status::OK();
}

folly::Future<Status> ExpandExecutor::execute() {
  maxSteps_ = expand_->maxSteps();
  sample_ = expand_->sample();
  stepLimits_ = expand_->stepLimits();
  NG_RETURN_IF_ERROR(buildRequestVids());
  if (nextStepVids_.empty()) {
    DataSet emptyDs;
    return finish(ResultBuilder().value(Value(std::move(emptyDs))).build());
  }
  if (maxSteps_ == 0) {
    DataSet ds;
    ds.colNames = expand_->colNames();
    for (const auto& vid : nextStepVids_) {
      Row row;
      row.values.emplace_back(vid);
      if (expand_->joinInput()) {
        row.values.emplace_back(vid);
      }
      ds.rows.emplace_back(std::move(row));
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).build());
  }
  if (expand_->joinInput() || !stepLimits_.empty()) {
    return getNeighbors();
  }
  return GetDstBySrc();
}

folly::Future<Status> ExpandExecutor::GetDstBySrc() {
  currentStep_++;
  time::Duration getDstTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(expand_->space(),
                                          qctx_->rctx()->session()->id(),
                                          qctx_->plan()->id(),
                                          qctx_->plan()->isProfileEnabled());
  std::vector<Value> vids(nextStepVids_.size());
  std::move(nextStepVids_.begin(), nextStepVids_.end(), vids.begin());
  return storageClient->getDstBySrc(param, std::move(vids), expand_->edgeTypes())
      .via(runner())
      .ensure([this, getDstTime]() {
        SCOPED_TIMER(&execTime_);
        addState("total_rpc_time", getDstTime);
      })
      .thenValue([this](StorageRpcResponse<GetDstBySrcResponse>&& resps) {
        memory::MemoryCheckGuard guard;
        nextStepVids_.clear();
        SCOPED_TIMER(&execTime_);
        auto& hostLatency = resps.hostLatency();
        for (size_t i = 0; i < hostLatency.size(); ++i) {
          size_t size = 0u;
          auto& result = resps.responses()[i];
          if (result.dsts_ref().has_value()) {
            size = (*result.dsts_ref()).size();
          }
          auto info = util::collectRespProfileData(result.result, hostLatency[i], size);
          addState(folly::sformat("step{} resp [{}]", currentStep_, i), info);
        }
        auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
        if (!result.ok()) {
          return folly::makeFuture<Status>(result.status());
        }
        auto& responses = resps.responses();
        if (currentStep_ < maxSteps_) {
          for (auto& resp : responses) {
            auto* dataset = resp.get_dsts();
            if (dataset == nullptr) continue;
            for (auto& row : dataset->rows) {
              nextStepVids_.insert(row.values.begin(), row.values.end());
            }
          }
          if (nextStepVids_.empty()) {
            DataSet emptyDs;
            finish(ResultBuilder().value(Value(std::move(emptyDs))).build());
            return folly::makeFuture<Status>(Status::OK());
          }
          return GetDstBySrc();
        } else {
          ResultBuilder builder;
          builder.state(result.value());
          DataSet ds;
          for (auto& resp : responses) {
            auto* dataset = resp.get_dsts();
            if (dataset == nullptr) continue;
            dataset->colNames = expand_->colNames();
            ds.append(std::move(*dataset));
          }
          builder.value(Value(std::move(ds))).iter(Iterator::Kind::kSequential);
          finish(builder.build());
          return folly::makeFuture<Status>(Status::OK());
        }
      });
}

folly::Future<Status> ExpandExecutor::getNeighbors() {
  currentStep_++;
  StorageClient* storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(expand_->space(),
                                          qctx_->rctx()->session()->id(),
                                          qctx_->plan()->id(),
                                          qctx_->plan()->isProfileEnabled());
  std::vector<Value> vids(nextStepVids_.size());
  std::move(nextStepVids_.begin(), nextStepVids_.end(), vids.begin());
  auto limit = stepLimits_.empty() ? -1 : stepLimits_[currentStep_ - 1];
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     {},
                     storage::cpp2::EdgeDirection::OUT_EDGE,
                     nullptr,
                     nullptr,
                     expand_->edgeProps(),
                     nullptr,
                     false,
                     sample_,
                     std::vector<storage::cpp2::OrderBy>(),
                     limit,
                     nullptr,
                     nullptr)
      .via(runner())
      .thenValue([this](RpcResponse&& resp) mutable {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        nextStepVids_.clear();
        SCOPED_TIMER(&execTime_);
        addStats(resp);
        time::Duration expandTime;
        return handleResponse(std::move(resp)).ensure([this, expandTime]() {
          std::string timeName = "graphExpandTime+" + folly::to<std::string>(currentStep_);
          addState(timeName, expandTime);
        });
      })
      .thenValue([this](Status s) -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(s);
        if (qctx()->isKilled()) {
          return Status::Error("Execution had been killed");
        }
        if (currentStep_ < maxSteps_) {
          if (!nextStepVids_.empty()) {
            return getNeighbors();
          }
          if (!preVisitedVids_.empty()) {
            return expandFromCache();
          }
        }
        return buildResult();
      });
}

folly::Future<Status> ExpandExecutor::expandFromCache() {
  for (; currentStep_ < maxSteps_; ++currentStep_) {
    time::Duration expandTime;
    if (qctx()->isKilled()) {
      return Status::Error("Execution had been killed");
    }
    std::unordered_map<Value, std::unordered_set<Value>> dst2VidsMap;
    std::unordered_set<Value> visitedVids;

    getNeighborsFromCache(dst2VidsMap, visitedVids);
    dst2VidsMap.swap(preDst2VidsMap_);
    visitedVids.swap(preVisitedVids_);
    std::string timeName = "graphCacheExpandTime+" + folly::to<std::string>(currentStep_);
    addState(timeName, expandTime);
    if (!nextStepVids_.empty()) {
      return getNeighbors();
    }
  }
  return buildResult();
}

void ExpandExecutor::getNeighborsFromCache(
    std::unordered_map<Value, std::unordered_set<Value>>& dst2VidsMap,
    std::unordered_set<Value>& visitedVids) {
  for (const auto& vid : preVisitedVids_) {
    auto findVid = adjDsts_.find(vid);
    if (findVid == adjDsts_.end()) {
      continue;
    }
    auto& dsts = findVid->second;

    for (const auto& dst : dsts) {
      updateDst2VidsMap(dst2VidsMap, vid, dst);
      if (currentStep_ >= maxSteps_) {
        continue;
      }
      if (adjDsts_.find(dst) == adjDsts_.end()) {
        nextStepVids_.emplace(dst);
      } else {
        visitedVids.emplace(dst);
      }
    }
  }
}

// 1、 update adjDsts_, cache vid and the corresponding dsts
// 2、 get next step's vids
// 3、 cache already visited vids
// 4、 get the dsts corresponding to the vid that has been visited in the previous step by adjDsts_
folly::Future<Status> ExpandExecutor::handleResponse(RpcResponse&& resps) {
  NG_RETURN_IF_ERROR(handleCompleteness(resps, FLAGS_accept_partial_success));
  std::unordered_map<Value, std::unordered_set<Value>> dst2VidsMap;
  std::unordered_set<Value> visitedVids;

  for (auto& resp : resps.responses()) {
    auto dataset = resp.get_vertices();
    if (!dataset) {
      continue;
    }
    for (GetNbrsRespDataSetIter iter(dataset); iter.valid(); iter.next()) {
      auto dsts = iter.getAdjDsts();
      if (dsts.empty()) {
        continue;
      }
      const auto& src = iter.getVid();
      // do not cache in the last step
      if (currentStep_ < maxSteps_) {
        adjDsts_.emplace(src, dsts);
      }

      for (const auto& dst : dsts) {
        updateDst2VidsMap(dst2VidsMap, src, dst);

        if (currentStep_ >= maxSteps_) {
          continue;
        }
        if (!stepLimits_.empty()) {
          // if stepLimits_ is not empty, do not use cache
          nextStepVids_.emplace(dst);
          continue;
        }

        if (adjDsts_.find(dst) == adjDsts_.end()) {
          nextStepVids_.emplace(dst);
        } else {
          visitedVids.emplace(dst);
        }
      }
    }
  }
  if (!preVisitedVids_.empty()) {
    getNeighborsFromCache(dst2VidsMap, visitedVids);
  }
  dst2VidsMap.swap(preDst2VidsMap_);
  visitedVids.swap(preVisitedVids_);
  return Status::OK();
}

void ExpandExecutor::updateDst2VidsMap(
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

folly::Future<Status> ExpandExecutor::buildResult() {
  DataSet ds;
  ds.colNames = expand_->colNames();
  for (auto pair : preDst2VidsMap_) {
    auto& dst = pair.first;
    for (auto src : pair.second) {
      Row row;
      row.values.emplace_back(src);
      row.values.emplace_back(dst);
      ds.rows.emplace_back(std::move(row));
    }
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
