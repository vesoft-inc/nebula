/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ALGO_MULTISHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_MULTISHORTESTPATHEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class MultiShortestPathExecutor final : public Executor {
 public:
  MultiShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("ProduceSemiShortestPath", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  // k: dst, v: paths to dst
  using Interims = std::unordered_map<Value, std::vector<Path>>;

  void init();
  void buildPath(Iterator* iter, Interims& currentPaths, bool reverse);
  bool conjunctPath(Interims& leftPaths, Interims& rightPaths, DataSet& ds);
  void setNextStepVid(Interims& paths, const string& var);

 private:
  size_t step_{1};
  std::string terminationVar_;
  std::unordered_multimap<Value, std::pair<Value, bool>> terminationMap_;
  Interims preLeftPaths_;
  Interims preRightPaths_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_MULTISHORTESTPATHEXECUTOR_H_
