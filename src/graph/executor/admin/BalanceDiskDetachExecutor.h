/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_BALANCEDISKDETACHEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_BALANCEDISKDETACHEXECUTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class BalanceDiskDetachExecutor final : public Executor {
 public:
  BalanceDiskDetachExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("BalanceDiskDetachExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_BALANCEDISKDETACHEXECUTOR_H_
