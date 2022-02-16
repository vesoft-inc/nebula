/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "graph/executor/Executor.h"  // for Executor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class PlanNode;
class QueryContext;

class RevokeRoleExecutor final : public Executor {
 public:
  RevokeRoleExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("RevokeRoleExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> revokeRole();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_
