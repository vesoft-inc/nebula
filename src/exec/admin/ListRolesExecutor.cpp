/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/ListRolesExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ListRolesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return listRoles();
}

folly::Future<Status> ListRolesExecutor::listRoles() {
    auto *lrNode = asNode<ListRoles>(node());
    return qctx()->getMetaClient()->listRoles(lrNode->space())
        .via(runner())
        .then([this](StatusOr<std::vector<meta::cpp2::RoleItem>> &&resp) {
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
                        meta::cpp2::_RoleType_VALUES_TO_NAMES.at(item.get_role_type())
                    }));
            }
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
