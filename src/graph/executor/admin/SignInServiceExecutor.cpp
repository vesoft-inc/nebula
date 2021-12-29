/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/SignInServiceExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SignInServiceExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return signInService();
}

folly::Future<Status> SignInServiceExecutor::signInService() {
  auto *siNode = asNode<SignInService>(node());
  auto type = siNode->type();
  return qctx()
      ->getMetaClient()
      ->signInService(type, siNode->clients())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Sign in service failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
