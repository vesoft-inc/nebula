/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "clients/storage/StorageClientBase.h"     // for StorageRpcResponse
#include "common/base/Status.h"                    // for Status
#include "common/datatypes/DataSet.h"              // for DataSet
#include "graph/executor/StorageAccessExecutor.h"  // for StorageAccessExecutor
#include "graph/planner/plan/Query.h"              // for GetNeighbors
#include "interface/gen-cpp2/storage_types.h"      // for GetNeighborsResponse

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;

class PlanNode;
class QueryContext;

class GetNeighborsExecutor final : public StorageAccessExecutor {
 public:
  GetNeighborsExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("GetNeighborsExecutor", node, qctx) {
    gn_ = asNode<GetNeighbors>(node);
  }

  folly::Future<Status> execute() override;

  DataSet buildRequestDataSet();

 private:
  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
  Status handleResponse(RpcResponse& resps);

 private:
  const GetNeighbors* gn_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
