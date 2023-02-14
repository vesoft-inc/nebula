// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_EXPAND_ALL_H_
#define GRAPH_EXECUTOR_QUERY_EXPAND_ALL_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
class ExpandAllExecutor final : public StorageAccessExecutor {
 public:
  ExpandAllExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ExpandAllExecutor", node, qctx) {
    expand_ = asNode<ExpandAll>(node);
  }

  Status buildRequestVids();

  folly::Future<Status> execute() override;

  folly::Future<Status> getNeighbors();

  void getNeighborsFromCache(std::unordered_set<Value>& visitedVids);

  folly::Future<Status> expandFromCache();

  void buildResult(const List& vList, const List& eList);

  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
  folly::Future<Status> handleResponse(RpcResponse&& resps);

 private:
  const ExpandAll* expand_;
  size_t currentStep_{0};
  size_t maxSteps_{0};
  DataSet result_;
  YieldColumns* edgeColumns_{nullptr};
  YieldColumns* vertexColumns_{nullptr};

  bool sample_{false};
  int64_t curLimit_{0};
  int64_t curMaxLimit_{std::numeric_limits<int64_t>::max()};
  std::vector<int64_t> limits_;

  std::unordered_set<Value> nextStepVids_;
  std::unordered_set<Value> preVisitedVids_;
  std::unordered_map<Value, std::vector<List>> adjList_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_ExpandAllExecutor_H_
