/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_
#define EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class SubgraphExecutor : public Executor {
public:
    SubgraphExecutor(const PlanNode* node, QueryContext* qctx)
        : Executor("SubgraphExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    void oneMoreStep();

private:
    std::unordered_set<Value>   historyVids_;
};

}   // namespace graph
}   // namespace nebula
#endif  // EXECUTOR_ALGO_SUBGRAPHEXECUTOR_H_
