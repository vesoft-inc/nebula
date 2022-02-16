/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_PERMISSIONMANAGER_H_
#define GRAPH_SERVICE_PERMISSIONMANAGER_H_

#include <string>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/thrift/ThriftTypes.h"
#include "graph/context/ValidateContext.h"
#include "graph/service/GraphFlags.h"
#include "graph/session/ClientSession.h"
#include "interface/gen-cpp2/meta_types.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {
class ClientSession;
class ValidateContext;

class PermissionManager final {
 public:
  PermissionManager() = delete;
  static Status canReadSpace(ClientSession *session, GraphSpaceID spaceId);
  static Status canReadSchemaOrData(ClientSession *session, ValidateContext *vctx);
  static Status canWriteSpace(ClientSession *session);
  static Status canWriteSchema(ClientSession *session, ValidateContext *vctx);
  static Status canWriteUser(ClientSession *session);
  static Status canReadUser(ClientSession *session, const std::string &targetUser);
  static Status canWriteRole(ClientSession *session,
                             meta::cpp2::RoleType targetRole,
                             GraphSpaceID spaceId,
                             const std::string &targetUser);
  static Status canWriteData(ClientSession *session, ValidateContext *vctx);

 private:
  static StatusOr<meta::cpp2::RoleType> checkRoleWithSpace(ClientSession *session,
                                                           ValidateContext *vctx);
};
}  // namespace graph
}  // namespace nebula

#endif  // COMMON_PERMISSION_PERMISSIONMANAGER_H_
