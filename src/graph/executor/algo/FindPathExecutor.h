/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_

#include "graph/executor/Executor.h"
// 1、keep path
// 2 extract dst
// 3 conjunct path
namespace nebula {
namespace graph {
class FindPathExecutor final : public Executor {
 public:
  FindPathExecutor(const PlanNode* node, QueryContext* qctx) : Executor("FindPath", node, qctx) {}

  folly::Future<Status> execute() override;

  // key : dstVid, value : map(key : srcVid, value : <path>)
  using SPInterimsMap = std::unordered_map<Value, std::unordered_map<Value, std::vector<Path>>>;
  using APInterimsMap = std::unordered_map<Value, std::vector<Path>>;

 private:
  std::vector<Path> createPaths(const std::vector<Path>& paths, const Edge& edge);
  void buildPath(std::vector<Path>& leftPaths, std::vector<Path>& rightPaths, DataSet& ds);
  bool conjunctPath(SPInterimsMap& leftPaths, SPInterimsMap& rightPaths, DataSet& ds);
  void setNextStepVidFromPath(SPInterimsMap& leftPaths, SPInterimsMap& rightPaths);

  // shortestPath
  void shortestPathInit();
  void shortestPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds);
  void doShortestPath(Iterator* iter, SPInterimsMap& currentPath, bool reverse);

  // allpath
  void allPathInit();
  void allPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds);
  void doAllPath(Iterator* iter, APInterimsMap& currentPath, bool reverse);
  void printPath(SPInterimsMap& paths);

 private:
  bool noLoop_{false};
  // current step
  size_t step_{1};
  // total steps
  size_t steps_{0};
  std::string terminationVar_;
  std::unordered_multimap<Value, Value> terminationMap_;
  SPInterimsMap historyLeftPaths_;
  SPInterimsMap historyRightPaths_;
  SPInterimsMap prePaths_;

  // allpath
  APInterimsMap historyAllLeftPaths_;
  APInterimsMap historyAllRightPaths_;
  APInterimsMap preAllPaths_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_
