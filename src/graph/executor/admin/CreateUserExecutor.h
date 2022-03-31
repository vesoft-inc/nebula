// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_CREATEUSEREXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_CREATEUSEREXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class CreateUserExecutor final : public Executor {
 public:
  CreateUserExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateUserExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> createUser();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_CREATEUSEREXECUTOR_H_
