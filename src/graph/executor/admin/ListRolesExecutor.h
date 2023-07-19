// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_LISTROLESEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_LISTROLESEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ListRolesExecutor final : public Executor {
 public:
  ListRolesExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ListRolesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> listRoles();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_LISTROLESEXECUTOR_H_
