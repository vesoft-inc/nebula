/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/GetNeighborsExecutor.h"

#include <folly/Format.h>              // for sformat
#include <folly/Try.h>                 // for Try::~Try<T>, Try:...
#include <folly/futures/Future.h>      // for Future::Future<T>
#include <folly/futures/Promise.h>     // for Promise::Promise<T>
#include <folly/futures/Promise.h>     // for PromiseException::...
#include <stddef.h>                    // for size_t
#include <thrift/lib/cpp2/FieldRef.h>  // for optional_field_ref

#include <algorithm>      // for max
#include <sstream>        // for operator<<, basic_...
#include <string>         // for string, basic_string
#include <tuple>          // for get
#include <unordered_map>  // for unordered_map, ope...
#include <utility>        // for move, __tuple_elem...
#include <vector>         // for vector

#include "clients/storage/StorageClient.h"         // for StorageClient, Sto...
#include "common/base/Logging.h"                   // for LogMessage, COMPAC...
#include "common/base/StatusOr.h"                  // for StatusOr
#include "common/datatypes/List.h"                 // for List
#include "common/datatypes/Value.h"                // for Value
#include "common/time/Duration.h"                  // for Duration
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator, Iterator...
#include "graph/context/QueryContext.h"            // for QueryContext
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/planner/plan/ExecutionPlan.h"      // for ExecutionPlan
#include "graph/planner/plan/PlanNode.h"           // for PlanNode
#include "graph/service/GraphFlags.h"              // for FLAGS_accept_parti...
#include "graph/service/RequestContext.h"          // for RequestContext
#include "graph/session/ClientSession.h"           // for ClientSession

using nebula::storage::StorageClient;
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
  StorageClient* storageClient = qctx_->getStorageClient();
  QueryExpressionContext qec(qctx()->ectx());
  StorageClient::CommonRequestParam param(gn_->space(),
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
