// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_CHANGEPASSWORDEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_CHANGEPASSWORDEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ChangePasswordExecutor final : public Executor {
 public:
  ChangePasswordExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ChangePasswordExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> changePassword();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_CHANGEPASSWORDEXECUTOR_H_
