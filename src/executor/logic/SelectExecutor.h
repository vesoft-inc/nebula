/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_LOGIC_SELECTEXECUTOR_H_
#define EXECUTOR_LOGIC_SELECTEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class SelectExecutor final : public Executor {
public:
    SelectExecutor(const PlanNode* node, QueryContext* qctx);

    folly::Future<Status> execute() override;

    void setThenBody(Executor* then) {
        then_ = DCHECK_NOTNULL(then);
    }

    void setElseBody(Executor* els) {
        else_ = DCHECK_NOTNULL(els);
    }

    Executor* thenBody() const {
        return then_;
    }

    Executor* elseBody() const {
        return else_;
    }

private:
    Executor* then_;
    Executor* else_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_LOGIC_SELECTEXECUTOR_H_
