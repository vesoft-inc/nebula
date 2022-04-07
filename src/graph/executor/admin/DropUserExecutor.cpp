// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/DropUserExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> DropUserExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return dropUser();
}

folly::Future<Status> DropUserExecutor::dropUser() {
  auto *duNode = asNode<DropUser>(node());
  return qctx()
      ->getMetaClient()
      ->dropUser(*duNode->username(), duNode->ifExist())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Drop user failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
