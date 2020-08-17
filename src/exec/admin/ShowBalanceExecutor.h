/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_SHOWBALANCEEXECUTOR_H_
#define EXEC_ADMIN_SHOWBALANCEEXECUTOR_H_

#include "exec/Executor.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

class ShowBalanceExecutor final : public Executor {
public:
    ShowBalanceExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ShowBalanceExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> showBalance();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXEC_ADMIN_SHOWBALANCEEXECUTOR_H_
