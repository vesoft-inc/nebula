/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
#define EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class BFSShortestPathExecutor final : public Executor {
public:
    BFSShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
        : Executor("BFSShortestPath", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    std::unordered_set<Value>               visited_;
};
}  // namespace graph
}  // namespace nebula
#endif   // EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
