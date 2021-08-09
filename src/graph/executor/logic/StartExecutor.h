/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_LOGIC_STARTEXECUTOR_H_
#define EXECUTOR_LOGIC_STARTEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class StartExecutor final : public Executor {
public:
    StartExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("StartExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_LOGIC_STARTEXECUTOR_H_
