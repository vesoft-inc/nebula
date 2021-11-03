/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SHOWBALANCEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SHOWBALANCEEXECUTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ShowBalanceExecutor final : public Executor {
 public:
  ShowBalanceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowBalanceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> showBalance();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SHOWBALANCEEXECUTOR_H_
