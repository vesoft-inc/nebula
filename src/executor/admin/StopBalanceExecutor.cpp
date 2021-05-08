/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/StopBalanceExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> StopBalanceExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return stopBalance();
}

folly::Future<Status> StopBalanceExecutor::stopBalance() {
    return qctx()->getMetaClient()->balance({}, true, false)
        .via(runner())
        .thenValue([this](StatusOr<int64_t> resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            DataSet v({"ID"});
            v.emplace_back(Row({resp.value()}));
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
