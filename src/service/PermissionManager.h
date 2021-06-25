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
#include "session/ClientSession.h"
#include "context/ValidateContext.h"

namespace nebula {
namespace graph {

class PermissionManager final {
public:
    PermissionManager() = delete;
    static Status canReadSpace(ClientSession *session, GraphSpaceID spaceId);
    static Status canReadSchemaOrData(ClientSession *session, ValidateContext *vctx);
    static Status canWriteSpace(ClientSession *session);
    static Status canWriteSchema(ClientSession *session, ValidateContext *vctx);
    static Status canWriteUser(ClientSession *session);
    static Status canWriteRole(ClientSession *session,
                               meta::cpp2::RoleType targetRole,
                               GraphSpaceID spaceId,
                               const std::string& targetUser);
    static Status canWriteData(ClientSession *session, ValidateContext *vctx);

private:
    static StatusOr<meta::cpp2::RoleType> checkRoleWithSpace(ClientSession *session,
                                                             ValidateContext *vctx);
};
}  // namespace graph
}  // namespace nebula

#endif   // COMMON_PERMISSION_PERMISSIONMANAGER_H_
