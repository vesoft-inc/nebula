/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_BALANCELEADERSEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_BALANCELEADERSEXECUTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class BalanceLeadersExecutor final : public Executor {
 public:
  BalanceLeadersExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("BaanceLeadersExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> balanceLeaders();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_BALANCELEADERSEXECUTOR_H_
