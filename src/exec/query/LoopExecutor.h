/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_LOOPEXECUTOR_H_
#define EXEC_QUERY_LOOPEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class LoopExecutor final : public SingleInputExecutor {
public:
    LoopExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *input, Executor *body)
        : SingleInputExecutor("LoopExecutor", node, ectx, input),
          body_(body) {}

    Status prepare() override;

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> iterate();

    bool toContinue();

    // Hold the last executor node of loop body executors chain
    Executor *body_;

    // Represent loop index. It will be updated and stored in ExecutionContext before starting loop
    // body. The mainly usage is that MultiOutputsExecutor could check whether current execution is
    // called multiple times.
    int64_t iterCount_{-1};
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_LOOPEXECUTOR_H_
