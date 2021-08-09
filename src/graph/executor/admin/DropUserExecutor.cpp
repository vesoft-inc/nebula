/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/DropUserExecutor.h"
#include "planner/plan/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> DropUserExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return dropUser();
}

folly::Future<Status> DropUserExecutor::dropUser() {
    auto *duNode = asNode<DropUser>(node());
    return qctx()->getMetaClient()->dropUser(*duNode->username(), duNode->ifExist())
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Drop user failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
