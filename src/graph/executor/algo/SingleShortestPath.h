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
  SingleShortestPath(const ShortestPath* node,
                     QueryContext* qctx,
                     std::unordered_map<std::string, std::string>* stats)
      : ShortestPathBase(node, qctx, stats) {}

  folly::Future<Status> execute(const std::unordered_set<Value>& startVids,
                                const std::unordered_set<Value>& endVids,
                                DataSet* result) override;

  using HalfPath = std::vector<std::unordered_map<DstVid, std::vector<CustomStep>>>;

 private:
  void init(const std::unordered_set<Value>& startVids,
            const std::unordered_set<Value>& endVids,
            size_t rowSize);

  folly::Future<Status> shortestPath(size_t rowNum, size_t stepNum);

  folly::Future<Status> getNeighbors(size_t rowNum, size_t stepNum, bool reverse);

  Status buildPath(size_t rowNum, RpcResponse&& resps, bool reverse);

  Status doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  folly::Future<Status> handleResponse(size_t rowNum, size_t stepNum);

  bool conjunctPath(size_t rowNum, size_t stepNum);

  bool buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids);

  void buildOddPath(size_t rowNum, const std::vector<Value>& meetVids);

  std::vector<Row> createRightPath(size_t rowNum, const Value& meetVid, bool evenStep);

  std::vector<Row> createLeftPath(size_t rowNum, const Value& meetVid);

 private:
  std::vector<std::unordered_set<Value>> leftVisitedVids_;
  std::vector<std::unordered_set<Value>> rightVisitedVids_;
  std::vector<HalfPath> allLeftPaths_;
  std::vector<HalfPath> allRightPaths_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SINGLE_SHORTESTPATH_H_
