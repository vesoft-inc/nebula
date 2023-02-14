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
  const auto& inputVar = gn_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iterRef();
  auto iter = static_cast<SequentialIter*>(inputIter);
  size_t iterSize = iter->size();
  vids_.reserve(iterSize);
  auto* src = gn_->src();
  QueryExpressionContext ctx(ectx_);

  const auto& spaceInfo = qctx()->rctx()->session()->space();
  const auto& metaVidType = *(spaceInfo.spaceDesc.vid_type_ref());
  auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
  for (; iter->valid(); iter->next()) {
    const auto& vid = src->eval(ctx(iter));
    DCHECK_EQ(vid.type(), vidType)
        << "Mismatched vid type: " << vid.type() << ", space vid type: " << vidType;
    if (vid.type() == vidType) {
      vids_.emplace(vid);
      initVids_.emplace_back(vid);
    }
  }
  return Status::OK();
}

folly::Future<Status> ExpandExecutor::execute() {
  NG_RETURN_IF_ERROR(buildRequestVids());
  if (vids_.empty()) {
    DataSet emptyDs;
    return finish(ResultBuilder().value(Value(std::move(emptyDs))).build());
  }
  return getNeighbors();
}

folly::Future<Status> ExpandExecutor::getNeighbors() {
  currentStep_++;
  time::Duration getNbrTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(gn_->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  std::vector<Value> vids(vids_.size());
  std::move(vids_.begin(), vids_.end(), vids.begin());
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     gn_->edgeTypes(),
                     gn_->edgeDirection(),
                     nullptr,
                     gn_->vertexProps(),
                     gn_->edgeProps(),
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
        vids_.clear();
        SCOPED_TIMER(&execTime_);
        // addStats(resp, getNbrTime.elapsedInUSec());
        time::Duration expandTime;
        return handleResponse(std::move(resp)).ensure([this, expandTime]() {
          otherStats_.emplace("expandTime", folly::sformat("{}(us)", expandTime.elapsedInUSec()));
        });
      })
      .thenValue([this](Status s) -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(s);
        if (currentStep_ < maxSteps_ && !vids_.empty()) {
          return getNeighbors();
        }
        return buildResult();
      });
}

folly::Future<Status> ExpandExecutor::handleResponse(RpcResponse&& resps) {
  NG_RETURN_IF_ERROR(handleCompleteness(resps, FLAGS_accept_partial_success));
  std::unordered_map<Value, std::unordered_map<Value, size_t>> dst2VidMap;
  for (auto& resp : resps.responses()) {
    auto dataset = resp.get_vertices();
    if (dataset) {
      for (GetNbrsRespDataSetIter iter(dataset); iter.valid(); iter.next()) {
        auto result = iter.getSrcAndAdjDsts();
        if (result.empty() || result.size() == 1) {
          continue;
        }

        vids_.insert(result.begin() + 1, result.end());
        auto& src = result.front();
        auto preIter = preDst2VidMap_.find(src);
        if (preIter == preDst2VidMap_.end()) {
          for (auto dstIter = result.begin() + 1; dstIter != result.end(); ++dstIter) {
            auto findDst = dst2VidMap.find(*dstIter);
            if (findDst == dst2VidMap.end()) {
              std::unordered_map<Value, size_t> tmp({{src, 1}});
              dst2VidMap.emplace(*dstIter, std::move(tmp));
            } else {
              auto findSrc = findDst->second.find(src);
              if (findSrc == findDst->second.end()) {
                findDst->second.emplace(src, 1);
              } else {
                findSrc->second++;
              }
            }
          }
        } else {
          for (auto dstIter = result.begin() + 1; dstIter != result.end(); ++dstIter) {
            auto findDst = dst2VidMap.find(*dstIter);
            if (findDst == dst2VidMap.end()) {
              dst2VidMap.emplace(*dstIter, preIter->second);
            } else {
              for (auto& preSrc : preIter->second) {
                auto findSrc = findDst->second.find(preSrc.first);
                if (findSrc == findDst->second.end()) {
                  findDst->second.emplace(preSrc.first, preSrc.second);
                } else {
                  findSrc->second += preSrc.second;
                }
              }
            }
          }
        }
      }
    }
  }
  //
  for (auto& pair : dst2VidMap) {
    auto& dst = pair.first;
    auto preFindDst = preDst2VidMap_.find(dst);
    if (preFindDst == preDst2VidMap_.end()) {
      continue;
    }
    for (auto& preSrc : preFindDst->second) {
      auto findSrc = pair.second.find(preSrc.first);
      if (findSrc == pair.second.end()) {
        pair.second.emplace(preSrc.first, preSrc.second);
      } else {
        findSrc->second += preSrc.second;
      }
    }
  }

  return Status::OK();
}

folly::Future<Status> ExpandExecutor::buildResult() {
  DataSet ds;
  ds.colNames = gn_->colNames();
  for (auto pair : preDst2VidMap_) {
    auto& dst = pair.first;
    for (auto srcPair : pair.second) {
      for (size_t i = 0; i < srcPair.second; ++i) {
        Row row;
        row.values.emplace_back(srcPair.first);
        row.values.emplace_back(dst);
        ds.rows.emplace_back(std::move(row));
      }
    }
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
