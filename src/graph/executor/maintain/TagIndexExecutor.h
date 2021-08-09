/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_MAINTAIN_TAGINDEXEXECUTOR_H_
#define EXECUTOR_MAINTAIN_TAGINDEXEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class CreateTagIndexExecutor final : public Executor {
public:
    CreateTagIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("CreateTagIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DropTagIndexExecutor final : public Executor {
public:
    DropTagIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DropTagIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DescTagIndexExecutor final : public Executor {
public:
    DescTagIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DescTagIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ShowCreateTagIndexExecutor final : public Executor {
public:
    ShowCreateTagIndexExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowCreateTagIndexExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ShowTagIndexesExecutor final : public Executor {
public:
    ShowTagIndexesExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowTagIndexesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ShowTagIndexStatusExecutor final : public Executor {
public:
    ShowTagIndexStatusExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowTagIndexStatusExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_MAINTAIN_TAGINDEXEXECUTOR_H_
