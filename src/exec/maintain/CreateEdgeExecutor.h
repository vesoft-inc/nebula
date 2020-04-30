/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MAINTAIN_CREATEEDGEEXECUTOR_H_
#define EXEC_MAINTAIN_CREATEEDGEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class CreateEdgeExecutor final : public Executor {
public:
    CreateEdgeExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("CreateEdgeExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> createEdge();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MAINTAIN_CREATEEDGEEXECUTOR_H_
