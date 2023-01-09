// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/GetDstBySrcExecutor.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetDstBySrcResponse;

namespace nebula {
namespace graph {

StatusOr<std::vector<Value>> GetDstBySrcExecutor::buildRequestList() {
  SCOPED_TIMER(&execTime_);
  auto inputVar = gd_->inputVar();
  auto iter = ectx_->getResult(inputVar).iter();
  return buildRequestListByVidType(iter.get(), gd_->src(), gd_->dedup());
}

folly::Future<Status> GetDstBySrcExecutor::execute() {
  auto res = buildRequestList();
  NG_RETURN_IF_ERROR(res);
  auto reqList = std::move(res).value();
  if (reqList.empty()) {
    DataSet emptyResult;
    emptyResult.colNames = gd_->colNames();
    return finish(ResultBuilder()
                      .value(Value(std::move(emptyResult)))
                      .iter(Iterator::Kind::kSequential)
                      .build());
  }

  time::Duration getDstTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  QueryExpressionContext qec(qctx()->ectx());
  StorageClient::CommonRequestParam param(gd_->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return storageClient->getDstBySrc(param, std::move(reqList), gd_->edgeTypes())
      .via(runner())
      .ensure([this, getDstTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc_time", folly::sformat("{}(us)", getDstTime.elapsedInUSec()));
      })
      .thenValue([this](StorageRpcResponse<GetDstBySrcResponse>&& resp) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        auto& hostLatency = resp.hostLatency();
        for (size_t i = 0; i < hostLatency.size(); ++i) {
          size_t size = 0u;
          auto& result = resp.responses()[i];
          if (result.dsts_ref().has_value()) {
            size = (*result.dsts_ref()).size();
          }
          auto info = util::collectRespProfileData(result.result, hostLatency[i], size);
          otherStats_.emplace(folly::sformat("resp[{}]", i), folly::toPrettyJson(info));
        }
        return handleResponse(resp, this->gd_->colNames());
      });
}

Status GetDstBySrcExecutor::handleResponse(RpcResponse& resps,
                                           const std::vector<std::string>& colNames) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  ResultBuilder builder;
  builder.state(result.value());

  auto& responses = resps.responses();
  DataSet ds;
  for (auto& resp : responses) {
    auto* dataset = resp.get_dsts();
    if (dataset == nullptr) {
      continue;
    }
    dataset->colNames = colNames;
    ds.append(std::move(*dataset));
  }
  builder.value(Value(std::move(ds))).iter(Iterator::Kind::kSequential);
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
