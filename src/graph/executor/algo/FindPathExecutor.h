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

  // key : dstVid, value: <path>
  using Interims = std::unordered_map<Value, std::vector<Path>>;

 private:
  void init();
  bool conjunctPath(Interims& leftPaths, Interims& rightPaths, DataSet& ds);
  void setNextStepVidFromPath(Interims& paths, const std::string& var);
  void buildPath(Iterator* iter, Interims& currentPath, bool reverse);

 private:
  bool shortest_{false};
  bool noLoop_{false};
  // current step
  size_t step_{1};
  // total steps
  size_t steps_{0};
  std::string terminationVar_;
  std::unordered_multimap<Value, std::pair<Value, bool>> termination_;

  Interims preLeftPaths_;
  Interims preRightPaths_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_FIND_PATH_EXECUTOR_H_
