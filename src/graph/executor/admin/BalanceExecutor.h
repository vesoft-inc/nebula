/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_BALANCEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_BALANCEEXECUTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class BalanceExecutor final : public Executor {
 public:
  BalanceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("BalanceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> balance();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_BALANCEEXECUTOR_H_
