/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXECUTOR_ADMIN_GROUPEXECUTOR_H_
#define EXECUTOR_ADMIN_GROUPEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class AddGroupExecutor final : public Executor {
public:
    AddGroupExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("AddGroupExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DropGroupExecutor final : public Executor {
public:
    DropGroupExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DropGroupExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DescribeGroupExecutor final : public Executor {
public:
    DescribeGroupExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DescribeGroupExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class AddZoneIntoGroupExecutor final : public Executor {
    public:
    AddZoneIntoGroupExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("AddZoneIntoGroupExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DropZoneFromGroupExecutor final : public Executor {
    public:
    DropZoneFromGroupExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DropZoneFromGroupExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ListGroupsExecutor final : public Executor {
public:
    ListGroupsExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ListGroupsExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_GROUPEXECUTOR_H_
