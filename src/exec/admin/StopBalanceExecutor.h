/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_STOPBALANCEEXECUTOR_H_
#define EXEC_ADMIN_STOPBALANCEEXECUTOR_H_

#include "exec/Executor.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

class StopBalanceExecutor final : public Executor {
public:
    StopBalanceExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("StopBalanceExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> stopBalance();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXEC_ADMIN_STOPBALANCEEXECUTOR_H_
