/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_ADMIN_SNAPSHOTEXECUTOR_H_
#define EXEC_ADMIN_SNAPSHOTEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class CreateSnapshotExecutor final : public Executor {
public:
    CreateSnapshotExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("CreateSnapshotExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> descSpace();
};

class DropSnapshotExecutor final : public Executor {
public:
    DropSnapshotExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("DropSnapshotExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> descSpace();
};

class ShowSnapshotsExecutor final : public Executor {
public:
    ShowSnapshotsExecutor(const PlanNode *node, QueryContext *ectx)
            : Executor("ShowSnapshotsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> descSpace();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_ADMIN_SNAPSHOTEXECUTOR_H_
