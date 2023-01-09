// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/GetNeighborsExecutor.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

StatusOr<std::vector<Value>> GetNeighborsExecutor::buildRequestVids() {
  SCOPED_TIMER(&execTime_);
  auto inputVar = gn_->inputVar();
  auto iter = ectx_->getResult(inputVar).iter();
  return buildRequestListByVidType(iter.get(), gn_->src(), gn_->dedup());
}

folly::Future<Status> GetNeighborsExecutor::execute() {
  auto res = buildRequestVids();
  NG_RETURN_IF_ERROR(res);
  auto vids = std::move(res).value();
  if (vids.empty()) {
    List emptyResult;
    return finish(ResultBuilder()
                      .value(Value(std::move(emptyResult)))
                      .iter(Iterator::Kind::kGetNeighbors)
                      .build());
  }

  time::Duration getNbrTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  QueryExpressionContext qec(qctx()->ectx());
  StorageClient::CommonRequestParam param(gn_->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     gn_->edgeTypes(),
                     gn_->edgeDirection(),
                     gn_->statProps(),
                     gn_->vertexProps(),
                     gn_->edgeProps(),
                     gn_->exprs(),
                     gn_->dedup(),
                     gn_->random(),
                     gn_->orderBy(),
                     gn_->limit(qec),
                     gn_->filter(),
                     nullptr)
      .via(runner())
      .ensure([this, getNbrTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc_time", folly::sformat("{}(us)", getNbrTime.elapsedInUSec()));
      })
      .thenValue([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
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
        return handleResponse(resp);
      });
}

Status GetNeighborsExecutor::handleResponse(RpcResponse& resps) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  ResultBuilder builder;
  builder.state(result.value());

  auto& responses = resps.responses();
  List list;
  for (auto& resp : responses) {
    auto dataset = resp.get_vertices();
    if (dataset == nullptr) {
      continue;
    }

    list.values.emplace_back(std::move(*dataset));
  }
  builder.value(Value(std::move(list))).iter(Iterator::Kind::kGetNeighbors);
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
