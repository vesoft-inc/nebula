/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
using nebula::storage::cpp2::GetNeighborsResponse;

class ShortestPathExecutor final : public StorageAccessExecutor {
 public:
  ShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ShortestPath", node, qctx) {
    pathNode_ = asNode<ShortestPath>(node);
    single_ = pathNode_->single();
    range_ = {pathNode_->stepRange()->min(), pathNode_->stepRange()->max()};
  }

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;

  Status init();
  folly::Future<GetNeighborsResponse> getNeighbors(bool reverse);

  folly::Future<Status> shortestPath(size_t i);
  folly::Future<Status> handleResponse(std::vector<RpcResponse>&& resps, size_t i);
  Status buildPath(RpcResponse& resp, bool reverse);
  Status doBuildPath(GetNeighborsIter* iter, bool reverse);
  bool conjunctPath();

 private:
  const ShortestPath* pathNode_{nullptr};
  size_t step_{0};
  std::pair<size_t, size_t> range_;
  bool single_{true};

  std::vector<std::pair<Value, Value>> cartesianProduct_;
  DataSet resultDs_;
  DataSet leftVids_;
  DataSet rightVids_;
  std::unordered_set<Value> leftVisitedVids_;
  std::unordered_set<Value> rightVisitedVids_;
  std::vector<std::unordered_map<Value, std::vector<Row>>> leftPaths_;
  std::vector<std::unordered_map<Value, std::vector<Row>>> rightPaths_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
