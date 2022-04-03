// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_ZONEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_ZONEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class MergeZoneExecutor final : public Executor {
 public:
  MergeZoneExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("MergeZoneExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class RenameZoneExecutor final : public Executor {
 public:
  RenameZoneExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("RenameZoneExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DropZoneExecutor final : public Executor {
 public:
  DropZoneExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DropZoneExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DivideZoneExecutor final : public Executor {
 public:
  DivideZoneExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DivideZoneExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DescribeZoneExecutor final : public Executor {
 public:
  DescribeZoneExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DescribeZoneExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class AddHostsIntoZoneExecutor final : public Executor {
 public:
  AddHostsIntoZoneExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("AddHostsIntoZoneExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ListZonesExecutor final : public Executor {
 public:
  ListZonesExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ListZonesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_ZONEEXECUTOR_H_
