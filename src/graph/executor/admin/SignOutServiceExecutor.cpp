// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/SignOutServiceExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SignOutServiceExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return signOutService();
}

folly::Future<Status> SignOutServiceExecutor::signOutService() {
  auto *siNode = asNode<SignOutService>(node());
  auto type = siNode->type();

  return qctx()->getMetaClient()->signOutService(type).via(runner()).thenValue(
      [this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Sign out service failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
