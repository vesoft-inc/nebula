/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SHOW_FT_INDEXES_EXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SHOW_FT_INDEXES_EXECUTOR_H_

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

class ShowFTIndexesExecutor final : public Executor {
 public:
  ShowFTIndexesExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowFTIndexesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class CreateFTIndexExecutor final : public Executor {
 public:
  CreateFTIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateFTIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DropFTIndexExecutor final : public Executor {
 public:
  DropFTIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DropFTIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SHOW_FT_INDEXES_EXECUTOR_H_
