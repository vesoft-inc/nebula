/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_MULTIOUTPUTSEXECUTOR_H_
#define EXECUTOR_QUERY_MULTIOUTPUTSEXECUTOR_H_

#include <unordered_map>

#include <folly/SpinLock.h>
#include <folly/futures/SharedPromise.h>

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class MultiOutputsExecutor final : public Executor {
public:
    MultiOutputsExecutor(const PlanNode *node, QueryContext* qctx)
        : Executor("MultiOutputsExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_QUERY_MULTIOUTPUTSEXECUTOR_H_
