/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/query/GetNeighborsExecutor.h"

#include <sstream>

#include "clients/storage/GraphStorageClient.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Vertex.h"
#include "graph/context/QueryContext.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/ScopedTimer.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

DataSet GetNeighborsExecutor::buildRequestDataSet() {
  SCOPED_TIMER(&execTime_);
  auto inputVar = gn_->inputVar();
  VLOG(1) << node()->outputVar() << " : " << inputVar;
  auto iter = ectx_->getResult(inputVar).iter();
  return buildRequestDataSetByVidType(iter.get(), gn_->src(), gn_->dedup());
}

folly::Future<Status> GetNeighborsExecutor::execute() {
  DataSet reqDs = buildRequestDataSet();
  if (reqDs.rows.empty()) {
    List emptyResult;
    return finish(ResultBuilder()
                      .value(Value(std::move(emptyResult)))
                      .iter(Iterator::Kind::kGetNeighbors)
                      .build());
  }

  time::Duration getNbrTime;
  GraphStorageClient* storageClient = qctx_->getStorageClient();
  QueryExpressionContext qec(qctx()->ectx());
  GraphStorageClient::CommonRequestParam param(gn_->space(),
                                               qctx()->rctx()->session()->id(),
                                               qctx()->plan()->id(),
                                               qctx()->plan()->isProfileEnabled());
  return storageClient
      ->getNeighbors(param,
                     std::move(reqDs.colNames),
                     std::move(reqDs.rows),
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
                     gn_->filter())
      .via(runner())
      .ensure([this, getNbrTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc_time", folly::sformat("{}(us)", getNbrTime.elapsedInUSec()));
      })
      .thenValue([this](StorageRpcResponse<GetNeighborsResponse>&& resp) {
        SCOPED_TIMER(&execTime_);
        auto& hostLatency = resp.hostLatency();
        for (size_t i = 0; i < hostLatency.size(); ++i) {
          size_t size = 0u;
          auto& result = resp.responses()[i];
          if (result.vertices_ref().has_value()) {
            size = (*result.vertices_ref()).size();
          }
          auto& info = hostLatency[i];
          otherStats_.emplace(
              folly::sformat("{} exec/total/vertices", std::get<0>(info).toString()),
              folly::sformat("{}(us)/{}(us)/{},", std::get<1>(info), std::get<2>(info), size));
          auto detail = getStorageDetail(result.result.latency_detail_us_ref());
          if (!detail.empty()) {
            otherStats_.emplace("storage_detail", detail);
          }
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
      LOG(INFO) << "Empty dataset in response";
      continue;
    }

    list.values.emplace_back(std::move(*dataset));
  }
  builder.value(Value(std::move(list))).iter(Iterator::Kind::kGetNeighbors);
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
