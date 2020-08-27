/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_PASSTHROUGHEXECUTOR_H_
#define EXEC_QUERY_PASSTHROUGHEXECUTOR_H_

#include <unordered_map>

#include <folly/SpinLock.h>
#include <folly/futures/SharedPromise.h>

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class PassThroughExecutor final : public Executor {
public:
    PassThroughExecutor(const PlanNode *node, QueryContext* qctx)
        : Executor("PassThroughExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_PASSTHROUGHEXECUTOR_H_
