// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_ALGO_BATCHSHORTESTPATH_H_
#define GRAPH_EXECUTOR_ALGO_BATCHSHORTESTPATH_H_

#include "graph/executor/algo/ShortestPathBase.h"
#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {

class BatchShortestPath final : public ShortestPathBase {
 public:
  BatchShortestPath(const ShortestPath* node,
                    QueryContext* qctx,
                    std::unordered_map<std::string, std::string>* stats)
      : ShortestPathBase(node, qctx, stats) {}

  folly::Future<Status> execute(const HashSet& startVids,
                                const HashSet& endVids,
                                DataSet* result) override;

  using CustomPath = Row;
  using PathMap = robin_hood::unordered_flat_map<
      DstVid,
      robin_hood::unordered_flat_map<StartVid, std::vector<CustomPath>, std::hash<StartVid>>,
      std::hash<DstVid>>;

 private:
  size_t splitTask(const HashSet& startVids, const HashSet& endVids);

  size_t init(const HashSet& startVids, const HashSet& endVids);

  folly::Future<Status> getNeighbors(size_t rowNum, size_t stepNum, bool reverse);

  folly::Future<Status> shortestPath(size_t rowNum, size_t stepNum);

  folly::Future<Status> handleResponse(size_t rowNum, size_t stepNum);

  Status buildPath(size_t rowNum, RpcResponse&& resp, bool reverse);

  Status doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse);

  folly::Future<bool> conjunctPath(size_t rowNum, bool oddStep);

  folly::Future<std::vector<Value>> getMeetVids(size_t rowNum,
                                                bool oddStep,
                                                std::vector<Value>& meetVids);

  void doConjunctPath(const std::vector<CustomPath>& leftPaths,
                      const std::vector<CustomPath>& rightPaths,
                      const Value& commonVertex,
                      size_t rowNum);

  std::vector<Row> createPaths(const std::vector<CustomPath>& paths, const CustomPath& path);

  void setNextStepVid(const PathMap& paths, size_t rowNum, bool reverse);

 private:
  std::vector<std::vector<StartVid>> batchStartVids_;
  std::vector<std::vector<EndVid>> batchEndVids_;
  std::vector<std::unordered_multimap<StartVid, std::pair<EndVid, bool>>> terminationMaps_;
  std::vector<PathMap> allLeftPathMaps_;
  std::vector<PathMap> allRightPathMaps_;

  std::vector<PathMap> currentLeftPathMaps_;
  std::vector<PathMap> currentRightPathMaps_;
  std::vector<PathMap> preRightPathMaps_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ALGO_BATCHSHORTESTPATH_H_
