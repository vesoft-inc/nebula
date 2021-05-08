/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/interface/gen-cpp2/meta_types.h"

#include "context/QueryContext.h"
#include "executor/admin/GrantRoleExecutor.h"
#include "planner/plan/Admin.h"
#include "service/PermissionManager.h"

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
    item.set_space_id(spaceId);   // TODO(shylock) pass space name directly
    item.set_user_id(*grNode->username());
    item.set_role_type(grNode->role());
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

}   // namespace graph
}   // namespace nebula
