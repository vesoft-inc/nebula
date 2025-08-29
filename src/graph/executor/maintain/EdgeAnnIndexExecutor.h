// Copyright (c) 2025 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_MAINTAIN_EDGENNINDEXEXECUTOR_H_
#define GRAPH_EXECUTOR_MAINTAIN_EDGEANNINDEXEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class CreateEdgeAnnIndexExecutor final : public Executor {
 public:
  CreateEdgeAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateEdgeAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DropEdgeAnnIndexExecutor final : public Executor {
 public:
  DropEdgeAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DropEdgeAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DescEdgeAnnIndexExecutor final : public Executor {
 public:
  DescEdgeAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DescEdgeAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowEdgeAnnIndexStatusExecutor final : public Executor {
 public:
  ShowEdgeAnnIndexStatusExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowEdgeAnnIndexStatusExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowCreateEdgeAnnIndexExecutor final : public Executor {
 public:
  ShowCreateEdgeAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowCreateEdgeAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowEdgeAnnIndexesExecutor final : public Executor {
 public:
  ShowEdgeAnnIndexesExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowEdgeAnnIndexesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_MAINTAIN_TAGANNINDEXEXECUTOR_H_
