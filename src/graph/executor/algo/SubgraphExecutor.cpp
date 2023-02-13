// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/SubgraphExecutor.h"

#include "common/memory/MemoryTracker.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
namespace nebula {
namespace graph {

folly::Future<Status> SubgraphExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  totalSteps_ = subgraph_->steps();
  auto iter = ectx_->getResult(subgraph_->inputVar()).iter();
  auto res = buildRequestListByVidType(iter.get(), subgraph_->src(), true);
  NG_RETURN_IF_ERROR(res);
  vids_ = res.value();
  if (vids_.empty()) {
    DataSet emptyResult;
    return finish(ResultBuilder().value(Value(std::move(emptyResult))).build());
  }
  return getNeighbors();
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
                     std::move(vids_),
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
                     currentStep_ == 1 ? subgraph_->edgeFilter() : subgraph_->filter(),
                     currentStep_ == 1 ? nullptr : subgraph_->tagFilter())
      .via(runner())
      .thenValue([this, getNbrTime](RpcResponse&& resp) mutable {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        otherStats_.emplace("total_rpc_time", folly::sformat("{}(us)", getNbrTime.elapsedInUSec()));
        auto& hostLatency = resp.hostLatency();
        for (size_t i = 0; i < hostLatency.size(); ++i) {
          size_t size = 0u;
          auto& result = resp.responses()[i];
          if (result.vertices_ref().has_value()) {
            size = (*result.vertices_ref()).size();
          }
          auto info = util::collectRespProfileData(result.result, hostLatency[i], size);
          otherStats_.emplace(folly::sformat("resp[{}]", i), folly::toPrettyJson(info));
        }
        vids_.clear();
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

  if (!process(std::move(iter)) || ++currentStep_ > totalSteps_) {
    filterEdges(0);
    return folly::makeFuture<Status>(Status::OK());
  } else {
    return getNeighbors();
  }
}

void SubgraphExecutor::filterEdges(int version) {
  auto iter = ectx_->getVersionedResult(subgraph_->outputVar(), version).iter();
  auto* iterPtr = iter.get();
  if (iterPtr->isGetNeighborsIter()) {
    auto* gnIter = static_cast<GetNeighborsIter*>(iterPtr);
    while (gnIter->valid()) {
      const auto& dst = gnIter->getEdgeProp("*", nebula::kDst);
      if (validVids_.find(dst) == validVids_.end()) {
        auto edge = gnIter->getEdge();
        gnIter->erase();
      } else {
        gnIter->next();
      }
    }
    gnIter->reset();
  }
  ResultBuilder builder;
  builder.iter(std::move(iter));
  ectx_->setVersionedResult(subgraph_->outputVar(), builder.build(), version);
}

bool SubgraphExecutor::process(std::unique_ptr<GetNeighborsIter> iter) {
  auto gnSize = iter->numRows();
  if (gnSize == 0) {
    return false;
  }

  ResultBuilder builder;
  builder.value(iter->valuePtr());

  HashMap currentVids;
  currentVids.reserve(gnSize);
  historyVids_.reserve(historyVids_.size() + gnSize);
  auto startVids = iter->vids();
  if (currentStep_ == 1) {
    for (auto& startVid : startVids) {
      currentVids.emplace(startVid, 0);
    }
  }
  validVids_.insert(std::make_move_iterator(startVids.begin()),
                    std::make_move_iterator(startVids.end()));
  auto& biDirectEdgeTypes = subgraph_->biDirectEdgeTypes();
  while (iter->valid()) {
    const auto& dst = iter->getEdgeProp("*", nebula::kDst);
    if (dst.empty()) {
      // no edge, dst is empty
      iter->next();
      continue;
    }
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
        vids_.emplace_back(std::move(dst));
      }
      iter->next();
    }
  }
  iter->reset();
  builder.iter(std::move(iter));
  finish(builder.build());
  // update historyVids
  historyVids_.insert(std::make_move_iterator(currentVids.begin()),
                      std::make_move_iterator(currentVids.end()));
  if (currentStep_ != 1 && subgraph_->tagFilter()) {
    filterEdges(-1);
  }
  if (vids_.empty()) {
    return false;
  }
  return true;
}

}  // namespace graph
}  // namespace nebula
