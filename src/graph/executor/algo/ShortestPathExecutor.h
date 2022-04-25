// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Algo.h"

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
  size_t buildRequestDataSet();

  folly::Future<Status> getNeighbors(size_t rowNum, bool reverse);

  folly::Future<Status> shortestPath(size_t rowNum, size_t stepNum);

  folly::Future<Status> handleResponse(size_t rowNum, size_t stepNum);

  Status handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices);

  Status buildPath(size_t rowNum, RpcResponse&& resp, bool reverse);

  folly::Future<Status> getMeetVidsProps(const std::vector<Value>& meetVids,
                                         std::vector<Value>& meetVertices);

  Status doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  bool conjunctPath(size_t rowNum, size_t stepNum);

  bool buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids);

  void buildOddPath(size_t rowNum, const std::vector<Value>& meetVids);

  std::vector<Row> createRightPath(size_t rowNum, const Value& meetVid, bool evenStep);

  std::vector<Row> createLeftPath(size_t rowNum, const Value& meetVid);

 private:
  const ShortestPath* pathNode_{nullptr};
  size_t maxStep_;
  bool single_{true};

  std::vector<DataSet> resultDs_;
  std::vector<DataSet> leftVids_;
  std::vector<DataSet> rightVids_;
  std::vector<std::unordered_set<Value>> leftVisitedVids_;
  std::vector<std::unordered_set<Value>> rightVisitedVids_;
  std::vector<std::vector<std::unordered_map<Value, std::vector<Row>>>> allLeftSteps_;
  std::vector<std::vector<std::unordered_map<Value, std::vector<Row>>>> allRightSteps_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SHORTESTPATHEXECUTOR_H_
