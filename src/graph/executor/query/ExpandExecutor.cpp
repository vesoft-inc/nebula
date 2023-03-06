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
  if (expand_->joinInput() || !limits_.empty()) {
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
        otherStats_.emplace("total_rpc_time", folly::sformat("{}(us)", getDstTime.elapsedInUSec()));
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
          otherStats_.emplace(folly::sformat("resp[{}]", i), folly::toPrettyJson(info));
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
            limits_.empty() ? std::numeric_limits<int64_t>::max() : limits_[currentStep_ - 1];
        return handleResponse(std::move(resp)).ensure([this, expandTime]() {
          otherStats_.emplace("expandTime", folly::sformat("{}(us)", expandTime.elapsedInUSec()));
        });
      })
      .thenValue([this](Status s) -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(s);
        if (qctx()->isKilled()) {
          return Status::OK();
        }
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
    if (qctx()->isKilled()) {
      return Status::OK();
    }
    curLimit_ = 0;
    curMaxLimit_ =
        limits_.empty() ? std::numeric_limits<int64_t>::max() : limits_[currentStep_ - 1];
    std::unordered_map<Value, std::unordered_set<Value>> dst2VidsMap;
    std::unordered_set<Value> visitedVids;

    std::vector<int64_t> samples;
    if (sample_) {
      int64_t size = 0;
      for (auto& vid : preVisitedVids_) {
        size += adjDsts_[vid].size();
      }
      algorithm::ReservoirSampling<int64_t> sampler(curMaxLimit_, size);
      samples = sampler.samples();
      std::sort(samples.begin(), samples.end(), [](int64_t a, int64_t b) { return a > b; });
    }

    getNeighborsFromCache(dst2VidsMap, visitedVids, samples);
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
    std::unordered_set<Value>& visitedVids,
    std::vector<int64_t>& samples) {
  for (const auto& vid : preVisitedVids_) {
    auto findVid = adjDsts_.find(vid);
    if (findVid == adjDsts_.end()) {
      continue;
    }
    auto& dsts = findVid->second;

    for (const auto& dst : dsts) {
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
  std::vector<int64_t> samples;
  if (sample_) {
    size_t size = 0;
    for (auto& resp : resps.responses()) {
      auto dataset = resp.get_vertices();
      if (!dataset) continue;
      GetNbrsRespDataSetIter iter(dataset);
      size += iter.size();
    }
    algorithm::ReservoirSampling<int64_t> sampler(curMaxLimit_, size);
    samples = sampler.samples();
    std::sort(samples.begin(), samples.end(), [](int64_t a, int64_t b) { return a > b; });
  }

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

      for (const auto& dst : dsts) {
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
    getNeighborsFromCache(dst2VidsMap, visitedVids, samples);
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
  DLOG(ERROR) << "expand result : " << ds.toString();
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
