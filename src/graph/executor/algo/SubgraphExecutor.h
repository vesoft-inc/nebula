/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>         // for allocator
#include <unordered_set>  // for unordered_set

#include "common/datatypes/Value.h"   // for Value, hash
#include "graph/executor/Executor.h"  // for Executor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class PlanNode;
class QueryContext;

class SubgraphExecutor : public Executor {
 public:
  SubgraphExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("SubgraphExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  void oneMoreStep();

 private:
  std::unordered_set<Value> historyVids_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_
