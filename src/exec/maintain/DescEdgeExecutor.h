/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MAINTAIN_DESCEDGEEXECUTOR_H_
#define EXEC_MAINTAIN_DESCEDGEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class DescEdgeExecutor final : public Executor {
public:
    DescEdgeExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DescEdgeExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> descEdge();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MAINTAIN_DESCEDGEEXECUTOR_H_
