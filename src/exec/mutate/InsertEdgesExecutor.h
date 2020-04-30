/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MUTATE_INSERTEDGEEXECUTOR_H_
#define EXEC_MUTATE_INSERTEDGEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class InsertEdgesExecutor final : public Executor {
public:
    InsertEdgesExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("InsertEdgesExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> insertEdges();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_INSERTEDGEEXECUTOR_H_
