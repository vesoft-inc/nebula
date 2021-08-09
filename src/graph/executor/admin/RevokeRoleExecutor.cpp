/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/interface/gen-cpp2/meta_types.h"

#include "context/QueryContext.h"
#include "executor/admin/RevokeRoleExecutor.h"
#include "planner/plan/Admin.h"
#include "service/PermissionManager.h"

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
    item.set_space_id(spaceId);
    item.set_user_id(*rrNode->username());
    item.set_role_type(rrNode->role());
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

}   // namespace graph
}   // namespace nebula
