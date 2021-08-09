/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXECUTOR_ADMIN_SPACESEXECUTOR_H_
#define EXECUTOR_ADMIN_SPACESEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class CreateSpaceExecutor final : public Executor {
public:
    CreateSpaceExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("CreateSpaceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DescSpaceExecutor final : public Executor {
public:
    DescSpaceExecutor(const PlanNode *node, QueryContext *qctx)
            : Executor("DescSpaceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DropSpaceExecutor final : public Executor {
public:
    DropSpaceExecutor(const PlanNode *node, QueryContext *qctx)
            : Executor("DropSpaceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ShowSpacesExecutor final : public Executor {
public:
    ShowSpacesExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowSpacesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ShowCreateSpaceExecutor final : public Executor {
public:
    ShowCreateSpaceExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowCreateSpaceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};
}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_SPACESEXECUTOR_H_
