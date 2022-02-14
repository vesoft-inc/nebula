/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class SubgraphExecutor : public Executor {
 public:
  SubgraphExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("SubgraphExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  std::unordered_map<Value, int64_t> historyVids_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_
