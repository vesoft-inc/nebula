/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>
#include "service/PermissionManager.h"

namespace nebula {
namespace graph {

// static
Status PermissionManager::canReadSpace(ClientSession *session, GraphSpaceID spaceId) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto roleResult = session->roleWithSpace(spaceId);
    if (!roleResult.ok()) {
        return Status::PermissionError("No permission to read space.");
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST : {
            return Status::OK();
        }
    }
    return Status::PermissionError("No permission to read space.");
}

// static
Status PermissionManager::canReadSchemaOrData(ClientSession *session, ValidateContext *vctx) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto roleResult = checkRoleWithSpace(session, vctx);
        if (!roleResult.ok()) {
        return Status::PermissionError("No permission to read schema/data.");
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST : {
            return Status::OK();
        }
    }
    return Status::PermissionError("No permission to read schema/data.");
}

// static
Status PermissionManager::canWriteSpace(ClientSession *session) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    return Status::PermissionError("No permission to write space.");
}

// static
Status PermissionManager::canWriteSchema(ClientSession *session, ValidateContext *vctx) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto roleResult = checkRoleWithSpace(session, vctx);
    if (!roleResult.ok()) {
        return Status::PermissionError("No permission to write schema.");
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA : {
            return Status::OK();
        }
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST :
            return Status::PermissionError("No permission to write schema.");
    }
    return Status::PermissionError("No permission to write schema.");
}

// static
Status PermissionManager::canWriteUser(ClientSession *session) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    // Cloud auth user cannot create user
    if (FLAGS_auth_type == "cloud") {
        return Status::PermissionError("Cloud authenticate user can't write user.");
    }
    if (session->isGod()) {
        return Status::OK();
    } else {
        return Status::PermissionError("No permission to write user.");
    }
}

Status PermissionManager::canWriteRole(ClientSession *session,
                                       meta::cpp2::RoleType targetRole,
                                       GraphSpaceID spaceId,
                                       const std::string& targetUser) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    // Cloud auth user cannot grant role
    if (FLAGS_auth_type == "cloud") {
        return Status::PermissionError("Cloud authenticate user can't write role.");
    }
    /**
     * Reject grant or revoke to himself.
     */
     if (session->user() == targetUser) {
         return Status::PermissionError("No permission to grant/revoke yourself.");
     }
    /*
     * Reject any user grant or revoke role to GOD
     */
    if (targetRole == meta::cpp2::RoleType::GOD) {
        return Status::PermissionError("No permission to grant/revoke god user.");
    }
    /*
     * God user can be grant or revoke any one.
     */
    if (session->isGod()) {
        return Status::OK();
    }
    /**
     * Only allow ADMIN user grant or revoke other user to DBA, USER, GUEST.
     */
    auto roleResult = session->roleWithSpace(spaceId);
    if (!roleResult.ok()) {
        return Status::PermissionError("No permission to write grant/revoke role.");
    }
    auto role = roleResult.value();
    if (role == meta::cpp2::RoleType::ADMIN && targetRole != meta::cpp2::RoleType::ADMIN) {
        return Status::OK();
    }
    return Status::PermissionError("No permission to grant/revoke `%s' to `%s'.",
                                   apache::thrift::util::enumNameSafe(role).c_str(),
                                   targetUser.c_str());
}

// static
Status PermissionManager::canWriteData(ClientSession *session, ValidateContext *vctx) {
    if (!FLAGS_enable_authorize) {
        return Status::OK();
    }
    if (session->isGod()) {
        return Status::OK();
    }
    auto roleResult = checkRoleWithSpace(session, vctx);
    if (!roleResult.ok()) {
        return Status::PermissionError("No permission to write data.");
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER : {
            return Status::OK();
        }
        case meta::cpp2::RoleType::GUEST :
            return Status::PermissionError("No permission to write data.");
    }
    return Status::PermissionError("No permission to write data.");
}

StatusOr<meta::cpp2::RoleType>
PermissionManager::checkRoleWithSpace(ClientSession *session, ValidateContext *vctx) {
    if (!vctx->spaceChosen()) {
        return Status::Error("No space chosen.");
    }
    if (vctx->whichSpace().id <= 0) {
        LOG(ERROR) << "Space id is invalid: " << vctx->whichSpace().id;
        return Status::PermissionError("No space chosen.");
    }
    auto roleResult = session->roleWithSpace(vctx->whichSpace().id);
    if (!roleResult.ok()) {
        return Status::PermissionError("No permission with the space.");
    }
    return roleResult.value();
}
}   // namespace graph
}   // namespace nebula
