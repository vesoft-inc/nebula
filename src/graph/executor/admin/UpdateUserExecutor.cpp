// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/UpdateUserExecutor.h"

#include <proxygen/lib/utils/CryptUtil.h>

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> UpdateUserExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return updateUser();
}

folly::Future<Status> UpdateUserExecutor::updateUser() {
  auto *uuNode = asNode<UpdateUser>(node());
  return qctx()
      ->getMetaClient()
      ->alterUser(*uuNode->username(), proxygen::md5Encode(folly::StringPiece(*uuNode->password())))
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Update user failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
