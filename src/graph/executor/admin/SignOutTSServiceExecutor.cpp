/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SignOutTSServiceExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SignOutTSServiceExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return signOutTSService();
}

folly::Future<Status> SignOutTSServiceExecutor::signOutTSService() {
    return qctx()->getMetaClient()->signOutFTService()
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Sign out text service failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
