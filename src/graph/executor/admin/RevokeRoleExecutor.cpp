// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/RevokeRoleExecutor.h"

#include "graph/planner/plan/Admin.h"
#include "graph/service/PermissionManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> RevokeRoleExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return revokeRole();
}

folly::Future<Status> RevokeRoleExecutor::revokeRole() {
  auto *rrNode = asNode<RevokeRole>(node());
  const auto *spaceName = rrNode->spaceName();
  auto spaceIdResult = qctx()->getMetaClient()->getSpaceIdByNameFromCache(*spaceName);
  NG_RETURN_IF_ERROR(spaceIdResult);
  auto spaceId = spaceIdResult.value();

  if (rrNode->role() == meta::cpp2::RoleType::GOD) {
    return Status::PermissionError("Permission denied");
  }
  auto *session = qctx_->rctx()->session();
  NG_RETURN_IF_ERROR(
      PermissionManager::canWriteRole(session, rrNode->role(), spaceId, *rrNode->username()));

  meta::cpp2::RoleItem item;
  item.space_id_ref() = spaceId;
  item.user_id_ref() = *rrNode->username();
  item.role_type_ref() = rrNode->role();
  return qctx()
      ->getMetaClient()
      ->revokeFromUser(std::move(item))
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Revoke role failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
