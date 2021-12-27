/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/ScanVerticesExecutor.h"

#include "common/time/ScopedTimer.h"
#include "graph/context/QueryContext.h"
#include "graph/util/SchemaUtil.h"

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
