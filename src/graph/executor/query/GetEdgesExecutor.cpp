/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/GetEdgesExecutor.h"

#include <folly/Format.h>     // for sformat
#include <folly/hash/Hash.h>  // for hash

#include <ostream>        // for operator<<, basic_...
#include <string>         // for string, basic_string
#include <tuple>          // for tuple, get, make_t...
#include <unordered_map>  // for operator!=, unorde...
#include <unordered_set>  // for unordered_set
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "clients/storage/StorageClient.h"         // for StorageClient, Sto...
#include "clients/storage/StorageClientBase.h"     // for StorageRpcResponse
#include "common/base/Base.h"                      // for kDst, kRank, kSrc
#include "common/base/Logging.h"                   // for CheckNotNull, LogM...
#include "common/base/Status.h"                    // for Status
#include "common/datatypes/Value.h"                // for Value, operator-
#include "common/expression/Expression.h"          // for Expression
#include "common/time/Duration.h"                  // for Duration
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator, operator<<
#include "graph/context/QueryContext.h"            // for QueryContext
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/planner/plan/ExecutionPlan.h"      // for ExecutionPlan
#include "graph/planner/plan/Query.h"              // for GetEdges
#include "graph/service/RequestContext.h"          // for RequestContext
#include "graph/session/ClientSession.h"           // for ClientSession
#include "interface/gen-cpp2/storage_types.h"      // for GetPropResponse

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
  return getEdges();
}

DataSet GetEdgesExecutor::buildRequestDataSet(const GetEdges *ge) {
  auto valueIter = ectx_->getResult(ge->inputVar()).iter();
  VLOG(1) << "GE input var:" << ge->inputVar() << " iter kind: " << valueIter->kind();
  QueryExpressionContext exprCtx(qctx()->ectx());

  nebula::DataSet edges({kSrc, kType, kRank, kDst});
  edges.rows.reserve(valueIter->size());
  std::unordered_set<std::tuple<Value, Value, Value, Value>> uniqueEdges;
  uniqueEdges.reserve(valueIter->size());
  for (; valueIter->valid(); valueIter->next()) {
    auto type = ge->type()->eval(exprCtx(valueIter.get()));
    auto src = ge->src()->eval(exprCtx(valueIter.get()));
    auto dst = ge->dst()->eval(exprCtx(valueIter.get()));
    auto rank = ge->ranking()->eval(exprCtx(valueIter.get()));
    type = type < 0 ? -type : type;
    auto edgeKey = std::make_tuple(src, type, rank, dst);
    if (ge->dedup() && !uniqueEdges.emplace(std::move(edgeKey)).second) {
      continue;
    }
    if (!rank.isInt()) {
      LOG(WARNING) << "wrong rank type";
      continue;
    }
    edges.emplace_back(Row({std::move(src), type, rank, std::move(dst)}));
  }
  return edges;
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
  SCOPED_TIMER(&execTime_);
  StorageClient *client = qctx()->getStorageClient();
  auto *ge = asNode<GetEdges>(node());
  if (ge->src() == nullptr || ge->type() == nullptr || ge->ranking() == nullptr ||
      ge->dst() == nullptr) {
    return Status::Error("ptr is nullptr");
  }

  auto edges = buildRequestDataSet(ge);

  if (edges.rows.empty()) {
    // TODO: add test for empty input.
    return finish(
        ResultBuilder().value(Value(DataSet(ge->colNames()))).iter(Iterator::Kind::kProp).build());
  }

  time::Duration getPropsTime;
  StorageClient::CommonRequestParam param(ge->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(client)
      ->getProps(param,
                 std::move(edges),
                 nullptr,
                 ge->props(),
                 ge->exprs(),
                 ge->dedup(),
                 ge->orderBy(),
                 ge->limit(qctx()),
                 ge->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this, ge](StorageRpcResponse<GetPropResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), ge->colNames());
      });
}

}  // namespace graph
}  // namespace nebula
