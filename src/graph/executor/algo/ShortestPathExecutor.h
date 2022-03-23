// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using RpcResponse = StorageRpcResponse<GetNeighborsResponse>;
using PropRpcResponse = StorageRpcResponse<nebula::storage::cpp2::GetPropResponse>;
namespace nebula {
namespace graph {
class ShortestPathExecutor final : public StorageAccessExecutor {
 public:
  ShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ShortestPath", node, qctx) {
    pathNode_ = asNode<ShortestPath>(node);
  }

  folly::Future<Status> execute() override;

 private:
  Status buildRequestDataSet();

  folly::Future<Status> getNeighbors(bool reverse);

  folly::Future<Status> shortestPath(size_t i);

  folly::Future<Status> handleResponse(size_t i);

  Status handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices);

  Status buildPath(RpcResponse&& resp, bool reverse);

  folly::Future<Status> getMeetVidsProps(const std::vector<Value>& meetVids,
                                         std::vector<Value>& meetVertices);

  Status doBuildPath(GetNeighborsIter* iter, bool reverse);

  bool conjunctPath();

  bool buildEvenPath(const std::vector<Value>& meetVids);

  void buildOddPath(const std::vector<Value>& meetVids);

  std::vector<Row> createRightPath(const Value& meetVid, bool evenStep);

  std::vector<Row> createLeftPath(const Value& meetVid);

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
  std::vector<std::unordered_map<Value, std::vector<Row>>> allLeftSteps_;
  std::vector<std::unordered_map<Value, std::vector<Row>>> allRightSteps_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
