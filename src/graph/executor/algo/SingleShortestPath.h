// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_QUERY_SINGLE_SHORTESTPATH_H_
#define GRAPH_EXECUTOR_QUERY_SINGLE_SHORTESTPATH_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Algo.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using RpcResponse = StorageRpcResponse<GetNeighborsResponse>;
using PropRpcResponse = StorageRpcResponse<nebula::storage::cpp2::GetPropResponse>;
namespace nebula {
namespace graph {
class SingleShortestPath final : public StorageAccessExecutor {
 public:
  SingleShortestPath(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("ShortestPath", node, qctx) {
    pathNode_ = asNode<ShortestPath>(node);
  }

  folly::Future<Status> execute() override;

  using DstVid = Value;
  // start vid of the path
  using StartVid = Value;
  // end vid of the path
  using EndVid = Value;
  // save the starting vertex and the corresponding edge. eg [vertex(a), edge(a->b)]
  using CustomStep = Row;
  using HalfPath = std::vector<std::unordered_map<DstVid, std::vector<CustomStep>>>;
  using VidMap = std::unordered_map<DstVid, std::unordered_set<StartVid>>;

 private:
  size_t initCartesianProduct();

  void singleShortestPathInit();

  void batchShortestPathInit();

  folly::Future<Status> getNeighbors(size_t rowNum, bool reverse);

  folly::Future<Status> singleShortestPath();

  folly::Future<Status> batchShortestPath();

  folly::Future<Status> shortestPath(size_t rowNum, size_t stepNum);

  folly::Future<Status> handleResponse(size_t rowNum, size_t stepNum);

  Status handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices);

  Status buildPath(size_t rowNum, RpcResponse&& resp, bool reverse);

  folly::Future<Status> getMeetVidsProps(const std::vector<Value>& meetVids,
                                         std::vector<Value>& meetVertices);

  Status buildSinglePairPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  Status buildMultiPairPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  bool conjunctPath(size_t rowNum, size_t stepNum);

  bool buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids);

  void buildOddPath(size_t rowNum, const std::vector<Value>& meetVids);

  std::vector<Row> createRightPath(size_t rowNum, const Value& meetVid, bool evenStep);

  std::vector<Row> createLeftPath(size_t rowNum, const Value& meetVid);

 private:
  const ShortestPath* pathNode_{nullptr};
  size_t maxStep_;
  bool singleShortest_{true};
  bool singlePair_{true};

  std::vector<DataSet> resultDs_;
  std::vector<DataSet> leftVids_;
  std::vector<DataSet> rightVids_;
  // only for single pair shortest path
  std::vector<std::unordered_set<Value>> leftVisitedVids_;
  std::vector<std::unordered_set<Value>> rightVisitedVids_;

  // only for multi pair shortest path
  std::vector<VidMap> allLeftVidMap_;
  std::vector<VidMap> allRightVidMap_;

  std::vector<HalfPath> allLeftPaths_;
  std::vector<HalfPath> allRightPaths_;

  std::unordered_set<std::pair<StartVid, EndVid>> cartesianProduct_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SINGLE_SHORTESTPATH_H_
