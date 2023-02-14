// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_EXPAND_H_
#define GRAPH_EXECUTOR_QUERY_EXPAND_H

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

// Get the dst id of the src id. The dst is is partially deduplicated on the storage side.
namespace nebula {
namespace graph {
class ExpandExecutor final : public StorageAccessExecutor {
 public:
  ExpandExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ExpandExecutor", node, qctx) {
    gn_ = asNode<GetNeighbors>(node);
  }

  Status buildRequestVids();

  folly::Future<Status> execute() override;

  folly::Future<Status> getNeighbors();

  folly::Future<Status> buildResult();

  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
  folly::Future<Status> handleResponse(RpcResponse&& resps);

 private:
  const GetNeighbors* gn_;
  size_t currentStep_{0};
  size_t maxSteps_{0};
  std::unordered_set<Value> vids_;
  std::vector<Value> initVids_;
  std::unordered_map<Value, std::unordered_map<Value, size_t>> preDst2VidMap_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_ExpandExecutor_H_
