// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_SIGNINSERVICEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SIGNINSERVICEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class SignInServiceExecutor final : public Executor {
 public:
  SignInServiceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SignInServiceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> signInService();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SIGNINSERVICEEXECUTOR_H_
