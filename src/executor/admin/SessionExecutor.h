/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_SESSIONEXECUTOR_H_
#define EXECUTOR_ADMIN_SESSIONEXECUTOR_H_

#include "common/time/TimeUtils.h"

#include "executor/Executor.h"
#include "service/RequestContext.h"

namespace nebula {
namespace graph {

class ShowSessionsExecutor final : public Executor {
public:
    ShowSessionsExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ShowSessionsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> listSessions();

    folly::Future<Status> getSession(SessionID sessionId);

    DateTime microSecToDateTime(int64_t microSec) {
        auto dateTime = time::TimeConversion::unixSecondsToDateTime(microSec / 1000000);
        dateTime.microsec = microSec % 1000000;
        return dateTime;
    }
};

class UpdateSessionExecutor final : public Executor {
public:
    UpdateSessionExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("UpdateSessionExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_SESSIONEXECUTOR_H_
