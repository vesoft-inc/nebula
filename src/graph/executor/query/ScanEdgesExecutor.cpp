// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/ScanEdgesExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/SchemaUtil.h"

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
      .thenValue([this](StorageRpcResponse<ScanResponse> &&rpcResp) {
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp), {});
      });
}

}  // namespace graph
}  // namespace nebula
