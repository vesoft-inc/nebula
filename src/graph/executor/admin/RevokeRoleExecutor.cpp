/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/RevokeRoleExecutor.h"

#include <folly/Try.h>                 // for Try::~Try<T>
#include <folly/futures/Future.h>      // for Future::Future<T>, Futu...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>
#include <folly/futures/Promise.h>     // for PromiseException::Promi...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>
#include <folly/futures/Promise.h>     // for PromiseException::Promi...
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <string>   // for basic_string
#include <utility>  // for move

#include "clients/meta/MetaClient.h"          // for MetaClient
#include "common/base/Status.h"               // for Status, NG_RETURN_IF_ERROR
#include "common/base/StatusOr.h"             // for StatusOr
#include "common/time/ScopedTimer.h"          // for SCOPED_TIMER
#include "graph/context/QueryContext.h"       // for QueryContext
#include "graph/planner/plan/Admin.h"         // for RevokeRole
#include "graph/service/PermissionManager.h"  // for PermissionManager
#include "graph/service/RequestContext.h"     // for RequestContext
#include "interface/gen-cpp2/meta_types.h"    // for RoleItem, RoleType, Rol...

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
  item.space_id_ref() = spaceId;
  item.user_id_ref() = *rrNode->username();
  item.role_type_ref() = rrNode->role();
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

}  // namespace graph
}  // namespace nebula
