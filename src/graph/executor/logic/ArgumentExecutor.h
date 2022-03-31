// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_LOGIC_ARGUMENTEXECUTOR_H
#define GRAPH_EXECUTOR_LOGIC_ARGUMENTEXECUTOR_H

#include "graph/executor/Executor.h"
// only used in match scenarios
// indicates the variable to be used as an argument to the right-hand side of an Apply operator.
// E.g MATCH (n)-[]-(l), (l)-[]-(m) return n,l,m
// MATCH (n)-[]-(l) MATCH (l)-[]-(m) return n,l,m
namespace nebula {
namespace graph {
class ArgumentExecutor final : public Executor {
 public:
  ArgumentExecutor(const PlanNode *node, QueryContext *qctx);

  folly::Future<Status> execute() override;

 private:
};

}  // namespace graph
}  // namespace nebula
#endif
