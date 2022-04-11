// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/GrantRoleExecutor.h"

#include "graph/planner/plan/Admin.h"
#include "graph/service/PermissionManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> GrantRoleExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return grantRole();
}

folly::Future<Status> GrantRoleExecutor::grantRole() {
  SCOPED_TIMER(&execTime_);
  auto *grNode = asNode<GrantRole>(node());
  const auto *spaceName = grNode->spaceName();
  auto spaceIdResult = qctx()->getMetaClient()->getSpaceIdByNameFromCache(*spaceName);
  NG_RETURN_IF_ERROR(spaceIdResult);
  auto spaceId = spaceIdResult.value();

  auto *session = qctx_->rctx()->session();
  NG_RETURN_IF_ERROR(
      PermissionManager::canWriteRole(session, grNode->role(), spaceId, *grNode->username()));

  meta::cpp2::RoleItem item;
  item.space_id_ref() = spaceId;  // TODO(shylock) pass space name directly
  item.user_id_ref() = *grNode->username();
  item.role_type_ref() = grNode->role();
  return qctx()
      ->getMetaClient()
      ->grantToUser(std::move(item))
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Grant role failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
