// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/ScanVerticesExecutor.h"

#include "graph/planner/plan/Query.h"
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
  if (sv->alwaysFalse()) {
    return finish(ResultBuilder().value(Value(DataSet(sv->colNames()))).build());
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
        addState("total_rpc", scanVertexTime);
      })
      .thenValue([this, sv](StorageRpcResponse<ScanResponse> &&rpcResp) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        addStats(rpcResp);
        return handleResp(std::move(rpcResp), sv->colNames());
      });
}

}  // namespace graph
}  // namespace nebula
