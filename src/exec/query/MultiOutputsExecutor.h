/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_MULTIOUTPUTSEXECUTOR_H
#define EXEC_QUERY_MULTIOUTPUTSEXECUTOR_H

#include <unordered_map>

#include <folly/SpinLock.h>
#include <folly/futures/SharedPromise.h>

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class MultiOutputsExecutor final : public SingleInputExecutor {
public:
    MultiOutputsExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *input)
        : SingleInputExecutor("MultiOutputsExecutor", node, ectx, input) {}

    Status prepare() override;

    folly::Future<Status> execute() override;

private:
    // This executor may be called parallelly by other executors depending on it. So it is necessary
    // to lock the execution for thread safety.
    folly::SpinLock lock_;

    // This shared promise to notify all other output executors to run except the guy calling this
    // executor firstly
    std::shared_ptr<folly::SharedPromise<Status>> sharedPromise_;

    bool prepared_{false};
    int32_t outCount_{0};
    int32_t currentOut_{0};
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_MULTIOUTPUTSEXECUTOR_H
