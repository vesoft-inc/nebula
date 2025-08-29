// Copyright (c) 2025 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_MAINTAIN_TAGANNINDEXEXECUTOR_H_
#define GRAPH_EXECUTOR_MAINTAIN_TAGANNINDEXEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class CreateTagAnnIndexExecutor final : public Executor {
 public:
  CreateTagAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateTagAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DropTagAnnIndexExecutor final : public Executor {
 public:
  DropTagAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DropTagAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DescTagAnnIndexExecutor final : public Executor {
 public:
  DescTagAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DescTagAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowCreateTagAnnIndexExecutor final : public Executor {
 public:
  ShowCreateTagAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowCreateTagAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowTagAnnIndexesExecutor final : public Executor {
 public:
  ShowTagAnnIndexesExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowTagAnnIndexesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowTagAnnIndexStatusExecutor final : public Executor {
 public:
  ShowTagAnnIndexStatusExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowTagAnnIndexStatusExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_MAINTAIN_TAGANNINDEXEXECUTOR_H_
