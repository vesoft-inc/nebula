/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/ScanEdgesExecutor.h"

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
#include "graph/planner/plan/Query.h"           // for ScanEdges
#include "graph/service/RequestContext.h"       // for RequestContext
#include "graph/session/ClientSession.h"        // for ClientSession
#include "interface/gen-cpp2/storage_types.h"   // for ScanResponse

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::ScanResponse;

namespace nebula {
namespace graph {

folly::Future<Status> ScanEdgesExecutor::execute() {
  return scanEdges();
}

folly::Future<Status> ScanEdgesExecutor::scanEdges() {
  SCOPED_TIMER(&execTime_);
  StorageClient *client = qctx()->getStorageClient();
  auto *se = asNode<ScanEdges>(node());
  if (se->limit() < 0) {
    return Status::Error(
        "Scan vertices or edges need to specify a limit number, "
        "or limit number can not push down.");
  }

  time::Duration scanEdgesTime;
  StorageClient::CommonRequestParam param(se->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(client)
      ->scanEdge(param, *DCHECK_NOTNULL(se->props()), se->limit(), se->filter())
      .via(runner())
      .ensure([this, scanEdgesTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc", folly::sformat("{}(us)", scanEdgesTime.elapsedInUSec()));
      })
      .thenValue([this, se](StorageRpcResponse<ScanResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), se->colNames());
      });
}

}  // namespace graph
}  // namespace nebula
