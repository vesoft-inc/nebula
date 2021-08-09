/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXECUTOR_ADMIN_ZONEEXECUTOR_H_
#define EXECUTOR_ADMIN_ZONEEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class AddZoneExecutor final : public Executor {
public:
    AddZoneExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("AddZoneExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DropZoneExecutor final : public Executor {
public:
    DropZoneExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DropZoneExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DescribeZoneExecutor final : public Executor {
public:
    DescribeZoneExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DescribeZoneExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class AddHostIntoZoneExecutor final : public Executor {
    public:
    AddHostIntoZoneExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("AddHostIntoZoneExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class DropHostFromZoneExecutor final : public Executor {
    public:
    DropHostFromZoneExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DropHostFromZoneExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

class ListZonesExecutor final : public Executor {
public:
    ListZonesExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ListZonesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_ADMIN_ZONEEXECUTOR_H_
