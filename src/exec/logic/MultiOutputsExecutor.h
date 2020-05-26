/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_MULTIOUTPUTSEXECUTOR_H_
#define EXEC_QUERY_MULTIOUTPUTSEXECUTOR_H_

#include <unordered_map>

#include <folly/SpinLock.h>
#include <folly/futures/SharedPromise.h>

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class MultiOutputsExecutor final : public Executor {
public:
    MultiOutputsExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("MultiOutputsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_MULTIOUTPUTSEXECUTOR_H_
