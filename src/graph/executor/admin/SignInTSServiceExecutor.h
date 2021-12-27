/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SIGNINTSSERVICEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SIGNINTSSERVICEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class SignInTSServiceExecutor final : public Executor {
 public:
  SignInTSServiceExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("SignInTSServiceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> signInTSService();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SIGNINTSSERVICEEXECUTOR_H_
