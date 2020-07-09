/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "service/PermissionManager.h"

namespace nebula {
namespace graph {

// static
bool PermissionManager::canReadSpace(Session *session, GraphSpaceID spaceId) {
    if (!FLAGS_enable_authorize) {
        return true;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    auto roleResult = session->roleWithSpace(spaceId);
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST : {
            havePermission = true;
            break;
        }
    }
    return havePermission;
}

// static
bool PermissionManager::canReadSchemaOrData(Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return false;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    auto roleResult = session->roleWithSpace(session->space());
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST : {
            havePermission = true;
            break;
        }
    }
    return havePermission;
}

// static
bool PermissionManager::canWriteSpace(Session *session) {
    return session->isGod();
}

// static
bool PermissionManager::canWriteSchema(Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return false;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    auto roleResult = session->roleWithSpace(session->space());
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA : {
            havePermission = true;
            break;
        }
        case meta::cpp2::RoleType::USER :
        case meta::cpp2::RoleType::GUEST :
            break;
    }
    return havePermission;
}

// static
bool PermissionManager::canWriteUser(Session *session) {
    return session->isGod();
}

bool PermissionManager::canWriteRole(Session *session,
                                     meta::cpp2::RoleType targetRole,
                                     GraphSpaceID spaceId,
                                     const std::string& targetUser) {
    if (!FLAGS_enable_authorize) {
        return true;
    }
    /**
     * Reject grant or revoke to himself.
     */
     if (session->user() == targetUser) {
         return false;
     }
    /*
     * Reject any user grant or revoke role to GOD
     */
    if (targetRole == meta::cpp2::RoleType::GOD) {
        return false;
    }
    /*
     * God user can be grant or revoke any one.
     */
    if (session->isGod()) {
        return true;
    }
    /**
     * Only allow ADMIN user grant or revoke other user to DBA, USER, GUEST.
     */
    auto roleResult = session->roleWithSpace(spaceId);
    if (!roleResult.ok()) {
        return false;
    }
    auto role = roleResult.value();
    if (role == meta::cpp2::RoleType::ADMIN && targetRole != meta::cpp2::RoleType::ADMIN) {
        return true;
    }
    return false;
}

// static
bool PermissionManager::canWriteData(Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return false;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    auto roleResult = session->roleWithSpace(session->space());
    if (!roleResult.ok()) {
        return havePermission;
    }
    auto role = roleResult.value();
    switch (role) {
        case meta::cpp2::RoleType::GOD :
        case meta::cpp2::RoleType::ADMIN :
        case meta::cpp2::RoleType::DBA :
        case meta::cpp2::RoleType::USER : {
            havePermission = true;
            break;
        }
        case meta::cpp2::RoleType::GUEST :
            break;
    }
    return havePermission;
}
}   // namespace graph
}   // namespace nebula
