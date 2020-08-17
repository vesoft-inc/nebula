/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/UpdateUserExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> UpdateUserExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return updateUser();
}

folly::Future<Status> UpdateUserExecutor::updateUser() {
    auto *uuNode = asNode<UpdateUser>(node());
    return qctx()->getMetaClient()->alterUser(*uuNode->username(), *uuNode->password())
        .via(runner())
        .then([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Update user failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
