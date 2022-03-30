// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

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
