// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_ALGO_SHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_SHORTESTPATHEXECUTOR_H_

#include <robin_hood.h>

#include "graph/executor/Executor.h"
#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {
class ShortestPathExecutor final : public Executor {
 public:
  using HashSet = robin_hood::unordered_flat_set<Value, std::hash<Value>>;
  ShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("ShortestPath", node, qctx) {
    pathNode_ = asNode<ShortestPath>(node);
  }

  folly::Future<Status> execute() override;

  size_t checkInput(HashSet& startVids, HashSet& endVids);

 private:
  const ShortestPath* pathNode_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ALGO_SHORTESTPATHEXECUTOR_H_
