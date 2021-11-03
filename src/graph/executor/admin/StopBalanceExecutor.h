/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_STOPBALANCEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_STOPBALANCEEXECUTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class StopBalanceExecutor final : public Executor {
 public:
  StopBalanceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("StopBalanceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> stopBalance();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_STOPBALANCEEXECUTOR_H_
