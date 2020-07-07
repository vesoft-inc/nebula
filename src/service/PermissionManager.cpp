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

/*static*/
bool PermissionManager::canShow(Session *session,
                                ShowSentence::ShowType type,
                                GraphSpaceID targetSpace) {
    if (!FLAGS_enable_authorize) {
        return true;
    }
    bool havePermission = false;
    switch (type) {
        case ShowSentence::ShowType::kShowParts:
        case ShowSentence::ShowType::kShowTags:
        case ShowSentence::ShowType::kShowEdges:
        case ShowSentence::ShowType::kShowTagIndexes:
        case ShowSentence::ShowType::kShowEdgeIndexes:
        case ShowSentence::ShowType::kShowCreateTag:
        case ShowSentence::ShowType::kShowCreateEdge:
        case ShowSentence::ShowType::kShowCreateTagIndex:
        case ShowSentence::ShowType::kShowCreateEdgeIndex:
        case ShowSentence::ShowType::kShowTagIndexStatus:
        case ShowSentence::ShowType::kShowEdgeIndexStatus: {
            /**
             * Above operations can get the space id via session,
             * so the permission same with canReadSchemaOrData.
             * They've been checked by "USE SPACE", so here skip the check.
             */
            havePermission = true;
            break;
        }
        case ShowSentence::ShowType::kShowCharset:
        case ShowSentence::ShowType::kShowCollation:
        case ShowSentence::ShowType::kShowHosts: {
            /**
             * all roles can be show for above operations.
             */
            havePermission = true;
            break;
        }
        case ShowSentence::ShowType::kShowSpaces:
        case ShowSentence::ShowType::kShowCreateSpace:
        case ShowSentence::ShowType::kShowRoles: {
            /*
             * Above operations are special operation.
             * can not get the space id via session,
             * Permission checking needs to be done in their executor.
             */
            havePermission = canReadSpace(session, targetSpace);
            break;
        }
        case ShowSentence::ShowType::kShowUsers:
        case ShowSentence::ShowType::kShowSnapshots: {
            /**
             * Only GOD role can be show.
             */
            havePermission = session->isGod();
            break;
        }
        case ShowSentence::ShowType::kUnknown:
            break;
    }
    return havePermission;
}

}   // namespace graph
}   // namespace nebula
