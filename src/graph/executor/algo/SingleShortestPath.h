// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_ALGO_SINGLE_SHORTESTPATH_H_
#define GRAPH_EXECUTOR_ALGO_SINGLE_SHORTESTPATH_H_

#include "graph/executor/algo/ShortestPathBase.h"
#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {

class SingleShortestPath final : public ShortestPathBase {
 public:
  using HashSet = robin_hood::unordered_flat_set<Value, std::hash<Value>>;
  SingleShortestPath(const ShortestPath* node,
                     QueryContext* qctx,
                     std::unordered_map<std::string, std::string>* stats)
      : ShortestPathBase(node, qctx, stats) {}

  folly::Future<Status> execute(const HashSet& startVids,
                                const HashSet& endVids,
                                DataSet* result) override;

  using HalfPath = std::vector<
      robin_hood::unordered_flat_map<DstVid, std::vector<CustomStep>, std::hash<Value>>>;

 private:
  void init(const HashSet& startVids, const HashSet& endVids, size_t rowSize);

  folly::Future<Status> shortestPath(size_t rowNum, size_t stepNum);

  folly::Future<Status> getNeighbors(size_t rowNum, size_t stepNum, bool reverse);

  Status buildPath(size_t rowNum, RpcResponse&& resps, bool reverse);

  Status doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  folly::Future<Status> handleResponse(size_t rowNum, size_t stepNum);

  folly::Future<bool> conjunctPath(size_t rowNum, size_t stepNum);

  folly::Future<bool> buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids);

  void buildOddPath(size_t rowNum, const std::vector<Value>& meetVids);

  std::vector<Row> createRightPath(size_t rowNum, const Value& meetVid, bool evenStep);

  std::vector<Row> createLeftPath(size_t rowNum, const Value& meetVid);

 private:
  std::vector<HashSet> leftVisitedVids_;
  std::vector<HashSet> rightVisitedVids_;
  std::vector<HalfPath> allLeftPaths_;
  std::vector<HalfPath> allRightPaths_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SINGLE_SHORTESTPATH_H_
