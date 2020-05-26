/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_GETVERTICESEXECUTOR_H_
#define EXEC_QUERY_GETVERTICESEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class GetVerticesExecutor final : public Executor {
public:
    GetVerticesExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("GetVerticesExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> getVertices();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_GETVERTICESEXECUTOR_H_
