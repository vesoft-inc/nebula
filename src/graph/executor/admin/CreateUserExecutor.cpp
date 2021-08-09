/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/encryption/MD5Utils.h"

#include "context/QueryContext.h"
#include "executor/admin/CreateUserExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateUserExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return createUser();
}

folly::Future<Status> CreateUserExecutor::createUser() {
    auto *cuNode = asNode<CreateUser>(node());
    return qctx()
        ->getMetaClient()
        ->createUser(*cuNode->username(),
                     encryption::MD5Utils::md5Encode(*cuNode->password()),
                     cuNode->ifNotExist())
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Create User failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
