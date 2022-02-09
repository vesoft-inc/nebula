/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class BFSShortestPathExecutor final : public Executor {
 public:
  BFSShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("BFSShortestPath", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  std::unordered_set<Value> visited_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
