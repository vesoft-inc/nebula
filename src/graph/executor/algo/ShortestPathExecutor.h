// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_ALGO_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALOG_SHORTESTPATHEXECUTOR_H_

#include "graph/executor/Executor.h"
#include "graph/planner/plan/Algo.h"
namespace nebula {
namespace graph {
class ShortestPathExecutor final : public Executor {
 public:
  ShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("ShortestPath", node, qctx) {
    pathNode_ = asNode<ShortestPath>(node);
  }

  folly::Future<Status> execute() override;

  size_t checkInput(std::unordered_set<Value>& startVids, std::unordered_set<Value>& endVids);

 private:
  const ShortestPath* pathNode_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ALGO_SHORTESTPATHEXECUTOR_H_
