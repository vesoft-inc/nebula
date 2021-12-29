/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SIGNOUTTSSERVICEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SIGNOUTTSSERVICEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class SignOutServiceExecutor final : public Executor {
 public:
  SignOutServiceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SignOutServiceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> signOutService();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SIGNOUTTSSERVICEEXECUTOR_H_
