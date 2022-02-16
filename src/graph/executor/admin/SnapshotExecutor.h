/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SNAPSHOTEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SNAPSHOTEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "graph/executor/Executor.h"  // for Executor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class PlanNode;
class QueryContext;

class CreateSnapshotExecutor final : public Executor {
 public:
  CreateSnapshotExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateSnapshotExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> descSpace();
};

class DropSnapshotExecutor final : public Executor {
 public:
  DropSnapshotExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DropSnapshotExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> descSpace();
};

class ShowSnapshotsExecutor final : public Executor {
 public:
  ShowSnapshotsExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowSnapshotsExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> descSpace();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SNAPSHOTEXECUTOR_H_
