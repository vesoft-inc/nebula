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
    CreateTagIndexExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("CreateTagIndexExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class DropTagIndexExecutor final : public Executor {
public:
    DropTagIndexExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("DropTagIndexExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class DescTagIndexExecutor final : public Executor {
public:
    DescTagIndexExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("DescTagIndexExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class RebuildTagIndexExecutor final : public Executor {
public:
    RebuildTagIndexExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("RebuildTagIndexExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_MAINTAIN_TAGINDEXEXECUTOR_H_
