/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ListRolesExecutor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <algorithm>    // for max, find_if
#include <string>       // for string, basic_string, ope...
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Status.h"             // for Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for DataSet, Row
#include "common/datatypes/Value.h"         // for Value
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/planner/plan/Admin.h"       // for ListRoles
#include "graph/service/RequestContext.h"   // for RequestContext
#include "graph/session/ClientSession.h"    // for ClientSession
#include "interface/gen-cpp2/meta_types.h"  // for RoleItem, RoleType, RoleT...

namespace nebula {
namespace graph {

folly::Future<Status> ListRolesExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return listRoles();
}

folly::Future<Status> ListRolesExecutor::listRoles() {
  auto *lrNode = asNode<ListRoles>(node());
  return qctx()
      ->getMetaClient()
      ->listRoles(lrNode->space())
      .via(runner())
      .thenValue([this](StatusOr<std::vector<meta::cpp2::RoleItem>> &&resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return std::move(resp).status();
        }
        nebula::DataSet v({"Account", "Role Type"});
        auto items = std::move(resp).value();
        // Only god and admin show all roles, other roles only show themselves
        const auto &account = qctx_->rctx()->session()->user();
        auto foundItem = std::find_if(items.begin(), items.end(), [&account](const auto &item) {
          return item.get_user_id() == account;
        });
        if (foundItem != items.end()) {
          if (foundItem->get_role_type() != meta::cpp2::RoleType::ADMIN) {
            v.emplace_back(Row({foundItem->get_user_id(),
                                apache::thrift::util::enumNameSafe(foundItem->get_role_type())}));
          } else {
            for (const auto &item : items) {
              v.emplace_back(nebula::Row(
                  {item.get_user_id(), apache::thrift::util::enumNameSafe(item.get_role_type())}));
            }
          }
        } else if (qctx_->rctx()->session()->isGod()) {
          for (const auto &item : items) {
            v.emplace_back(nebula::Row(
                {item.get_user_id(), apache::thrift::util::enumNameSafe(item.get_role_type())}));
          }
        }
        return finish(std::move(v));
      });
}

}  // namespace graph
}  // namespace nebula
