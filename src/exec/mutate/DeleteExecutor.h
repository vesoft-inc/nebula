/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MUTATE_DELETEVERTICESEXECUTOR_H_
#define EXEC_MUTATE_DELETEVERTICESEXECUTOR_H_

#include "exec/mutate/MutateExecutor.h"

namespace nebula {
namespace graph {

class DeleteVerticesExecutor final : public MutateExecutor {
public:
    DeleteVerticesExecutor(const PlanNode *node, QueryContext *qctx)
        : MutateExecutor("DeleteVerticesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> deleteVertices();
};

class DeleteEdgesExecutor final : public MutateExecutor {
public:
    DeleteEdgesExecutor(const PlanNode *node, QueryContext *qctx)
        : MutateExecutor("DeleteEdgesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> deleteEdges();
    Status prepareEdgeKeys(const EdgeType edgeType, const EdgeKeys *edgeKeys);
};
}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_DELETEVERTICESEXECUTOR_H_
