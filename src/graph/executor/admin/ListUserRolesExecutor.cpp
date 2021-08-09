/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "executor/admin/ListUserRolesExecutor.h"
#include "planner/plan/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ListUserRolesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return listUserRoles();
}

folly::Future<Status> ListUserRolesExecutor::listUserRoles() {
    auto *lurNode = asNode<ListUserRoles>(node());
    return qctx()->getMetaClient()->getUserRoles(*lurNode->username())
        .via(runner())
        .thenValue([this](StatusOr<std::vector<meta::cpp2::RoleItem>> &&resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                return std::move(resp).status();
            }
            nebula::DataSet v({"Account", "Role Type"});
            auto items = std::move(resp).value();
            for (const auto &item : items) {
                v.emplace_back(nebula::Row(
                    {
                        item.get_user_id(),
                        apache::thrift::util::enumNameSafe(item.get_role_type())
                    }));
            }
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
