// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_LISTUSERSEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_LISTUSERSEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ListUsersExecutor final : public Executor {
 public:
  ListUsersExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ListUsersExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> listUsers();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_LISTUSERSEXECUTOR_H_
