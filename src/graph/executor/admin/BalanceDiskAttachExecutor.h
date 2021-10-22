/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_BALANCEDISKATTACHEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_BALANCEDISKATTACHEXECUTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class BalanceDiskAttachExecutor final : public Executor {
 public:
  BalanceDiskAttachExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("BalanceDiskAttachExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_BALANCEDISKATTACHEXECUTOR_H_
