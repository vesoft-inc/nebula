// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_MUTATE_INSERTVERTICESEXECUTOR_H_
#define GRAPH_EXECUTOR_MUTATE_INSERTVERTICESEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"

namespace nebula {
namespace graph {

class InsertVerticesExecutor final : public StorageAccessExecutor {
 public:
  InsertVerticesExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("InsertVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> insertVertices();
};

class InsertEdgesExecutor final : public StorageAccessExecutor {
 public:
  InsertEdgesExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("InsertEdgesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> insertEdges();
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_MUTATE_INSERTVERTICESEXECUTOR_H_
