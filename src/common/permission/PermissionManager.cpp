/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "permission/PermissionManager.h"

namespace nebula {
namespace permission {

// static
bool PermissionManager::canReadSpace(session::Session *session, GraphSpaceID spaceId) {
    if (!FLAGS_enable_authorize) {
        return true;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    switch (session->roleWithSpace(spaceId)) {
        case session::Role::GOD :
        case session::Role::ADMIN :
        case session::Role::DBA :
        case session::Role::USER :
        case session::Role::GUEST : {
            havePermission = true;
            break;
        }
        case session::Role::INVALID_ROLE : {
            break;
        }
    }
    return havePermission;
}

// static
bool PermissionManager::canReadSchemaOrData(session::Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return false;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    switch (session->roleWithSpace(session->space())) {
        case session::Role::GOD :
        case session::Role::ADMIN :
        case session::Role::DBA :
        case session::Role::USER :
        case session::Role::GUEST : {
            havePermission = true;
            break;
        }
        case session::Role::INVALID_ROLE : {
            break;
        }
    }
    return havePermission;
}

// static
bool PermissionManager::canWriteSpace(session::Session *session) {
    return session->isGod();
}

// static
bool PermissionManager::canWriteSchema(session::Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return false;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    switch (session->roleWithSpace(session->space())) {
        case session::Role::GOD :
        case session::Role::ADMIN :
        case session::Role::DBA : {
            havePermission = true;
            break;
        }
        case session::Role::USER :
        case session::Role::GUEST :
        case session::Role::INVALID_ROLE : {
            break;
        }
    }
    return havePermission;
}

// static
bool PermissionManager::canWriteUser(session::Session *session) {
    // Cloud auth user cannot create user
    if (!FLAGS_auth_type.compare("cloud")) {
        return false;
    } else {
        return session->isGod();
    }
}

bool PermissionManager::canWriteRole(session::Session *session,
                                     session::Role targetRole,
                                     GraphSpaceID spaceId,
                                     const std::string& targetUser) {
    if (!FLAGS_enable_authorize) {
        return true;
    }

    // Cloud auth user cannot grant role
    if (!FLAGS_auth_type.compare("cloud")) {
        return false;
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
    if (targetRole == session::Role::GOD) {
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
    auto role = session->roleWithSpace(spaceId);
    if (role == session::Role::ADMIN && targetRole != session::Role::ADMIN) {
        return true;
    }
    return false;
}

// static
bool PermissionManager::canWriteData(session::Session *session) {
    if (session->space() == -1) {
        LOG(ERROR) << "The space name is not set";
        return false;
    }
    if (session->isGod()) {
        return true;
    }
    bool havePermission = false;
    switch (session->roleWithSpace(session->space())) {
        case session::Role::GOD :
        case session::Role::ADMIN :
        case session::Role::DBA :
        case session::Role::USER : {
            havePermission = true;
            break;
        }
        case session::Role::GUEST :
        case session::Role::INVALID_ROLE : {
            break;
        }
    }
    return havePermission;
}

// static
bool PermissionManager::canShow(session::Session *session,
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

}   // namespace permission
}   // namespace nebula
