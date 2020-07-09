/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_PERMISSIONMANAGER_H_
#define SERVICE_PERMISSIONMANAGER_H_

#include "common/base/Base.h"
#include "common/clients/meta/MetaClient.h"
#include "parser/AdminSentences.h"
#include "service/GraphFlags.h"
#include "service/Session.h"

namespace nebula {
namespace graph {

class PermissionManager final {
public:
    PermissionManager() = delete;
    static bool canReadSpace(Session *session, GraphSpaceID spaceId);
    static bool canReadSchemaOrData(Session *session);
    static bool canWriteSpace(Session *session);
    static bool canWriteSchema(Session *session);
    static bool canWriteUser(Session *session);
    static bool canWriteRole(Session *session,
                             meta::cpp2::RoleType targetRole,
                             GraphSpaceID spaceId,
                             const std::string& targetUser);
    static bool canWriteData(Session *session);
};
}  // namespace graph
}  // namespace nebula

#endif   // COMMON_PERMISSION_PERMISSIONMANAGER_H_
