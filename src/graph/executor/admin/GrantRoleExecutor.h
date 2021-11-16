/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_GRANTROLEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_GRANTROLEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class GrantRoleExecutor final : public Executor {
 public:
  GrantRoleExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("GrantRoleExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> grantRole();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_GRANTROLEEXECUTOR_H_
