/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/encryption/MD5Utils.h"

#include "context/QueryContext.h"
#include "executor/admin/ChangePasswordExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> ChangePasswordExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return changePassword();
}

folly::Future<Status> ChangePasswordExecutor::changePassword() {
    auto *cpNode = asNode<ChangePassword>(node());
    return qctx()
        ->getMetaClient()
        ->changePassword(*cpNode->username(),
                         encryption::MD5Utils::md5Encode(*cpNode->newPassword()),
                         encryption::MD5Utils::md5Encode(*cpNode->password()))
        .via(runner())
        .thenValue([this](StatusOr<bool> &&resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Change password failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
