/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_MAINTAIN_TAGINDEXEXECUTOR_H_
#define GRAPH_EXECUTOR_MAINTAIN_TAGINDEXEXECUTOR_H_

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

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_MAINTAIN_TAGINDEXEXECUTOR_H_
