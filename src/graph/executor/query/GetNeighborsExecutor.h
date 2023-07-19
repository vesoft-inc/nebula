// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

// get the attributes of the start point and edges specified by the user from the storage layer
// the returned result is a list<dataset>, the format of each dataset refers to the storage.thrift
namespace nebula {
namespace graph {
class GetNeighborsExecutor final : public StorageAccessExecutor {
 public:
  GetNeighborsExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("GetNeighborsExecutor", node, qctx) {
    gn_ = asNode<GetNeighbors>(node);
  }

  folly::Future<Status> execute() override;

  StatusOr<std::vector<Value>> buildRequestVids();

 private:
  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
  Status handleResponse(RpcResponse& resps);

 private:
  const GetNeighbors* gn_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
