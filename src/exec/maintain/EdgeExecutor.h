/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MAINTAIN_EDGEEXECUTOR_H_
#define EXEC_MAINTAIN_EDGEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class CreateEdgeExecutor final : public Executor {
public:
    CreateEdgeExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("CreateEdgeExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> createEdge();
};

class DescEdgeExecutor final : public Executor {
public:
    DescEdgeExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("DescEdgeExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> descEdge();
};

class DropEdgeExecutor final : public Executor {
public:
    DropEdgeExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("DropEdgeExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class ShowEdgesExecutor final : public Executor {
public:
    ShowEdgesExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("ShowEdgesExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class ShowCreateEdgeExecutor final : public Executor {
public:
    ShowCreateEdgeExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("ShowCreateEdgeExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class AlterEdgeExecutor final : public Executor {
public:
    AlterEdgeExecutor(const PlanNode *node, QueryContext *qctx)
            : Executor("AlterEdgeExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};
}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MAINTAIN_EDGEEXECUTOR_H_
