/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ListUsersExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Admin.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> ListUsersExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return listUsers();
}

folly::Future<Status> ListUsersExecutor::listUsers() {
  return qctx()->getMetaClient()->listUsers().via(runner()).thenValue(
      [this](StatusOr<std::map<std::string, meta::cpp2::UserDescItem>>&& resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return std::move(resp).status();
        }

        nebula::DataSet v({"Account", "Roles in spaces"});
        auto users = std::move(resp).value();
        for (auto& user : users) {
          meta::cpp2::UserDescItem userDescItem = user.second;
          auto spaceRoleMap = userDescItem.get_space_role_map();
          std::vector<std::string> rolesInSpacesStrVector;
          for (auto& iter : spaceRoleMap) {
            auto spaceNameResult = qctx_->schemaMng()->toGraphSpaceName(iter.first);
            if (!spaceNameResult.ok()) {
              if (iter.first == 0) {
                rolesInSpacesStrVector.emplace_back(folly::sformat(
                    "{} in ALL_SPACE", apache::thrift::util::enumNameSafe(iter.second)));
              } else {
                LOG(ERROR) << "Space name of " << iter.first << " no found";
                return Status::Error("Space not found");
              }
            } else {
              rolesInSpacesStrVector.emplace_back(
                  folly::sformat("{} in {}",
                                 apache::thrift::util::enumNameSafe(iter.second),
                                 spaceNameResult.value()));
            }
          }
          v.emplace_back(nebula::Row({user.first, folly::join(',', rolesInSpacesStrVector)}));
        }
        return finish(std::move(v));
      });
}

}  // namespace graph
}  // namespace nebula
