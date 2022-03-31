// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_LOGIC_STARTEXECUTOR_H_
#define GRAPH_EXECUTOR_LOGIC_STARTEXECUTOR_H_

#include "graph/executor/Executor.h"
// the leaf node of the execution plan does not depend on other operators, do nothing
namespace nebula {
namespace graph {

class StartExecutor final : public Executor {
 public:
  StartExecutor(const PlanNode *node, QueryContext *qctx) : Executor("StartExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_LOGIC_STARTEXECUTOR_H_
