// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_ASSIGNEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_ASSIGNEXECUTOR_H_

#include "graph/executor/Executor.h"
// assign value to variable
namespace nebula {
namespace graph {

class AssignExecutor final : public Executor {
 public:
  AssignExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("AssignExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_QUERY_ASSIGNEXECUTOR_H_
