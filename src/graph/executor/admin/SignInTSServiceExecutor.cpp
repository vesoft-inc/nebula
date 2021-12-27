/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/SignInTSServiceExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SignInTSServiceExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return signInTSService();
}

folly::Future<Status> SignInTSServiceExecutor::signInTSService() {
  auto* siNode = asNode<SignInTSService>(node());
  return qctx()
      ->getMetaClient()
      ->signInFTService(siNode->type(), siNode->clients())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Sign in text service failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
