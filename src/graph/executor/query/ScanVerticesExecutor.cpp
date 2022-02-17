/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/ScanVerticesExecutor.h"

#include <folly/Format.h>  // for sformat

#include <string>         // for string, basic_string
#include <tuple>          // for get
#include <unordered_map>  // for operator!=, unordered...
#include <utility>        // for move
#include <vector>         // for vector

#include "clients/storage/StorageClient.h"      // for StorageClient, Storag...
#include "clients/storage/StorageClientBase.h"  // for StorageRpcResponse
#include "common/base/Logging.h"                // for CheckNotNull, DCHECK_...
#include "common/base/Status.h"                 // for Status
#include "common/time/Duration.h"               // for Duration
#include "common/time/ScopedTimer.h"            // for SCOPED_TIMER
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/planner/plan/ExecutionPlan.h"   // for ExecutionPlan
#include "graph/planner/plan/Query.h"           // for ScanVertices
#include "graph/service/RequestContext.h"       // for RequestContext
#include "graph/session/ClientSession.h"        // for ClientSession
#include "interface/gen-cpp2/storage_types.h"   // for ScanResponse

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::ScanResponse;

namespace nebula {
namespace graph {

folly::Future<Status> ScanVerticesExecutor::execute() {
  return scanVertices();
}

folly::Future<Status> ScanVerticesExecutor::scanVertices() {
  SCOPED_TIMER(&execTime_);

  auto *sv = asNode<ScanVertices>(node());
  if (sv->limit() < 0) {
    return Status::Error(
        "Scan vertices or edges need to specify a limit number,"
        " or limit number can not push down.");
  }
  StorageClient *storageClient = qctx()->getStorageClient();

  time::Duration scanVertexTime;
  StorageClient::CommonRequestParam param(sv->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(storageClient)
      ->scanVertex(param, *DCHECK_NOTNULL(sv->props()), sv->limit(), sv->filter())
      .via(runner())
      .ensure([this, scanVertexTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", scanVertexTime.elapsedInUSec()));
      })
      .thenValue([this, sv](StorageRpcResponse<ScanResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), sv->colNames());
      });
}

}  // namespace graph
}  // namespace nebula
