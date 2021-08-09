/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_LOGIC_LOOPEXECUTOR_H_
#define EXECUTOR_LOGIC_LOOPEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class LoopExecutor final : public Executor {
public:
    LoopExecutor(const PlanNode *node, QueryContext *qctx);

    folly::Future<Status> execute() override;

    void setLoopBody(Executor *body) {
        body_ = DCHECK_NOTNULL(body);
    }

    Executor *loopBody() const {
        return body_;
    }

private:
    // Hold the last executor node of loop body executors chain
    Executor *body_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_LOGIC_LOOPEXECUTOR_H_
