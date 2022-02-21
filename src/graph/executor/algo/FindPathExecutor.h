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

private:
 using HistoryPath = std::unordered_map<Value, std::unordered_map<Value, std::vector<const Path*>>>;
 using CurrentPath = std::unordered_map<Value, std::unordered_map<Value, std::vector<Path>>>;
 using HistoryAllPath = std::unordered_map<Value, std::vector<const Path*>>;
 using CurrentAllPath = std::unordered_map<Value, std::vector<Path>>;

private:
  std::vector<Path> createPaths(const std::vector<const Path*>& paths, const Edge& edge);
  void buildPath(std::vector<Path>& leftPaths, std::vector<Path>& rightPaths, DataSet& ds);
  bool conjunctPath(CurrentPath& leftPaths, CurrentPath& rightPaths, DataSet& ds);
  void setNextStepVidFromPath(CurrentPath& leftPaths, CurrentPath& rightPaths);

  // shortestPath
  void shortestPathInit();
  void shortestPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds);
  void doShortestPath(Iterator* iter, CurrentPath& currentPath, bool reverse);
  void updateHistory(CurrentPath& paths, HistoryPath& historyPaths);

  // allpath
  void allPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds);
  void doAllPath(Iterator* iter, CurrentAllPath& currentPath, bool reverse);
  void updateAllHistory(CurrentAllPath& paths, HistoryAllPath& historyPaths);

private:
  bool noLoop_{false};
  // current step
  size_t step_{0};
  // total steps
  size_t steps_{0};
  std::string terminationVar_;
  std::unordered_multimap<Value, Value> terminationMap_;
  // key: dst, value: {key: src, value: paths}
  HistoryPath historyLeftPaths_;
  HistoryPath historyRightPaths_;
  CurrentPath prePaths_;

  // allpath
  HistoryAllPath historyAllLeftPaths_;
  HistoryAllPath historyAllRightPaths_;
  CurrentAllPath preAllPaths_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_
