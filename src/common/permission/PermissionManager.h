/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_PERMISSION_PERMISSIONMANAGER_H_
#define COMMON_PERMISSION_PERMISSIONMANAGER_H_

#include "base/Base.h"
#include "session/Session.h"
#include "meta/client/MetaClient.h"
#include "parser/Sentence.h"
#include "parser/UserSentences.h"
#include "parser/AdminSentences.h"
#include "graph/GraphFlags.h"

namespace nebula {
namespace permission {

class PermissionManager final {
public:
    PermissionManager() = delete;
    static bool canReadSpace(session::Session *session, GraphSpaceID spaceId);
    static bool canReadSchemaOrData(session::Session *session);
    static bool canWriteSpace(session::Session *session);
    static bool canWriteSchema(session::Session *session);
    static bool canWriteUser(session::Session *session);
    static bool canWriteRole(session::Session *session,
                             session::Role targetRole,
                             GraphSpaceID spaceId,
                             const std::string& targetUser);
    static bool canWriteData(session::Session *session);
    static bool canShow(session::Session *session,
                        ShowSentence::ShowType type,
                        GraphSpaceID targetSpace = -1);
};
}  // namespace permission
}  // namespace nebula

#endif   // COMMON_PERMISSION_PERMISSIONMANAGER_H_

