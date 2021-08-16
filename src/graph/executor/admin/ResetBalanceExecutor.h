/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_RESETBALANCEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_RESETBALANCEEXECUTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ResetBalanceExecutor final : public Executor {
 public:
  ResetBalanceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ResetBalanceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> resetBalance();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_RESETBALANCEEXECUTOR_H_
