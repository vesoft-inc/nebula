// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_GETDSTBYSRCEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETDSTBYSRCEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

// Get the dst id of the src id. The dst is is partially deduplicated on the storage side.
namespace nebula {
namespace graph {
class GetDstBySrcExecutor final : public StorageAccessExecutor {
 public:
  GetDstBySrcExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("GetDstBySrcExecutor", node, qctx) {
    gd_ = asNode<GetDstBySrc>(node);
  }

  folly::Future<Status> execute() override;

  StatusOr<std::vector<Value>> buildRequestList();

 private:
  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetDstBySrcResponse>;
  Status handleResponse(RpcResponse& resps, const std::vector<std::string>& colNames);

 private:
  const GetDstBySrc* gd_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETDSTBYSRCEXECUTOR_H_
