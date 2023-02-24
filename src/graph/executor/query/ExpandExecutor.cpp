// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/ExpandExecutor.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

Status ExpandExecutor::buildRequestVids() {
  SCOPED_TIMER(&execTime_);
  const auto& inputVar = expand_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iterRef();
  auto iter = static_cast<SequentialIter*>(inputIter);
  size_t iterSize = iter->size();
  nextStepVids_.reserve(iterSize);
  QueryExpressionContext ctx(ectx_);

  const auto& spaceInfo = qctx()->rctx()->session()->space();
  const auto& metaVidType = *(spaceInfo.spaceDesc.vid_type_ref());
  auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
  for (; iter->valid(); iter->next()) {
    const auto& vid = iter->getColumn(0);
    DCHECK_EQ(vid.type(), vidType)
        << "Mismatched vid type: " << vid.type() << ", space vid type: " << vidType;
    if (vid.type() == vidType) {
      nextStepVids_.emplace(vid);
    }
  }
  return Status::OK();
}

folly::Future<Status> ExpandExecutor::execute() {
  maxSteps_ = expand_->maxSteps();
  NG_RETURN_IF_ERROR(buildRequestVids());
  if (nextStepVids_.empty()) {
    DataSet emptyDs;
    return finish(ResultBuilder().value(Value(std::move(emptyDs))).build());
  }
  return getNeighbors();
}

folly::Future<Status> ExpandExecutor::getNeighbors() {
  currentStep_++;
  time::Duration getNbrTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(expand_->space(),
                                          qctx_->rctx()->session()->id(),
                                          qctx_->plan()->id(),
                                          qctx_->plan()->isProfileEnabled());
  std::vector<Value> vids(nextStepVids_.size());
  std::move(nextStepVids_.begin(), nextStepVids_.end(), vids.begin());
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
                     false,
                     std::vector<storage::cpp2::OrderBy>(),
                     -1,
                     nullptr,
                     nullptr)
      .via(runner())
      .thenValue([this, getNbrTime](RpcResponse&& resp) mutable {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        nextStepVids_.clear();
        SCOPED_TIMER(&execTime_);
        // addStats(resp, getNbrTime.elapsedInUSec());
        time::Duration expandTime;
        curLimit_ = 0;
        curMaxLimit_ =
            limits_.empty() ? std::numeric_limits<int64_t>::max() : limits_[currentStep_];
        return handleResponse(std::move(resp)).ensure([this, expandTime]() {
          otherStats_.emplace("expandTime", folly::sformat("{}(us)", expandTime.elapsedInUSec()));
        });
      })
      .thenValue([this](Status s) -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(s);
        if (currentStep_ < maxSteps_) {
          if (!nextStepVids_.empty()) {
            return getNeighbors();
          } else if (!preVisitedVids_.empty()) {
            return expandFromCache();
          } else {
            return buildResult();
          }
        } else {
          return buildResult();
        }
      });
}

folly::Future<Status> ExpandExecutor::expandFromCache() {
  for (; currentStep_ < maxSteps_; ++currentStep_) {
    curLimit_ = 0;
    curMaxLimit_ = limits_.empty() ? std::numeric_limits<int64_t>::max() : limits_[currentStep_];
    std::unordered_map<Value, std::unordered_set<Value>> dst2VidsMap;
    std::unordered_set<Value> visitedVids;
    getNeighborsFromCache(dst2VidsMap, visitedVids);
    dst2VidsMap.swap(preDst2VidsMap_);
    visitedVids.swap(preVisitedVids_);

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
    if (sample_) {
      sample(vid, dsts);
      continue;
    }
    // limit
    if (curLimit_ >= curMaxLimit_) {
      continue;
    }
    for (const auto& dst : dsts) {
      if (curLimit_++ >= curMaxLimit_) {
        break;
      }
      if (adjDsts_.find(dst) == adjDsts_.end()) {
        nextStepVids_.emplace(dst);
      } else {
        visitedVids.emplace(dst);
      }
      updateDst2VidsMap(dst2VidsMap, vid, dst);
    }
  }
}

// 1、 update adjDsts_, cache vid and the corresponding dsts
// 2、 get next step's vids
// 3、 handle the situation when limit OR sample exists
// 4、 cache already visited vids
// 5、 get the dsts corresponding to the vid that has been visited in the previous step by adjDsts_
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
      adjDsts_.emplace(src, dsts);

      // sample
      if (sample_) {
        sample(src, dsts);
        continue;
      }
      // limit
      if (curLimit_ >= curMaxLimit_) {
        continue;
      }
      for (const auto& dst : dsts) {
        if (curLimit_++ >= curMaxLimit_) {
          break;
        }
        if (adjDsts_.find(dst) == adjDsts_.end()) {
          nextStepVids_.emplace(dst);
        } else {
          visitedVids.emplace(dst);
        }
        updateDst2VidsMap(dst2VidsMap, src, dst);
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

void ExpandExecutor::sample(const Value& src, const std::unordered_set<Value>& dsts) {
  UNUSED(src);
  UNUSED(dsts);
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
  DLOG(ERROR) << "expand result : " << ds.toString();
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
