/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/GetVerticesExecutor.h"

#include <folly/Format.h>  // for sformat

#include <ostream>        // for operator<<, basic_ost...
#include <string>         // for string, basic_string
#include <tuple>          // for get
#include <unordered_map>  // for operator!=, unordered...
#include <utility>        // for move
#include <vector>         // for vector

#include "clients/storage/StorageClient.h"      // for StorageClient, Storag...
#include "clients/storage/StorageClientBase.h"  // for StorageRpcResponse
#include "common/base/Base.h"                   // for kVid
#include "common/base/Logging.h"                // for CheckNotNull, COMPACT...
#include "common/base/Status.h"                 // for Status
#include "common/datatypes/Value.h"             // for Value
#include "common/time/Duration.h"               // for Duration
#include "common/time/ScopedTimer.h"            // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"     // for ExecutionContext
#include "graph/context/Iterator.h"             // for operator<<, Iterator
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/context/Result.h"               // for ResultBuilder, Result
#include "graph/planner/plan/ExecutionPlan.h"   // for ExecutionPlan
#include "graph/planner/plan/Query.h"           // for GetVertices
#include "graph/service/RequestContext.h"       // for RequestContext
#include "graph/session/ClientSession.h"        // for ClientSession
#include "interface/gen-cpp2/storage_types.h"   // for GetPropResponse

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetVerticesExecutor::execute() {
  return getVertices();
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
  SCOPED_TIMER(&execTime_);

  auto *gv = asNode<GetVertices>(node());
  StorageClient *storageClient = qctx()->getStorageClient();

  DataSet vertices = buildRequestDataSet(gv);
  if (vertices.rows.empty()) {
    // TODO: add test for empty input.
    return finish(
        ResultBuilder().value(Value(DataSet(gv->colNames()))).iter(Iterator::Kind::kProp).build());
  }

  time::Duration getPropsTime;
  StorageClient::CommonRequestParam param(gv->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 gv->props(),
                 nullptr,
                 gv->exprs(),
                 gv->dedup(),
                 gv->orderBy(),
                 gv->limit(qctx()),
                 gv->filter())
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this, gv](StorageRpcResponse<GetPropResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), gv->colNames());
      });
}

DataSet GetVerticesExecutor::buildRequestDataSet(const GetVertices *gv) {
  if (gv == nullptr) {
    return nebula::DataSet({kVid});
  }
  // Accept Table such as | $a | $b | $c |... as input which one column indicate
  // src
  auto valueIter = ectx_->getResult(gv->inputVar()).iter();
  VLOG(3) << "GV input var: " << gv->inputVar() << " iter kind: " << valueIter->kind();
  return buildRequestDataSetByVidType(valueIter.get(), gv->src(), gv->dedup());
}

}  // namespace graph
}  // namespace nebula
