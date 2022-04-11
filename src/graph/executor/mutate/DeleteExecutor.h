// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_MUTATE_DELETEVERTICESEXECUTOR_H_
#define GRAPH_EXECUTOR_MUTATE_DELETEVERTICESEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"

namespace nebula {
namespace graph {

class DeleteVerticesExecutor final : public StorageAccessExecutor {
 public:
  DeleteVerticesExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("DeleteVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> deleteVertices();
};

// delete the specified tag at the specified vertex
class DeleteTagsExecutor final : public StorageAccessExecutor {
 public:
  DeleteTagsExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("DeleteTagsExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> deleteTags();
};

class DeleteEdgesExecutor final : public StorageAccessExecutor {
 public:
  DeleteEdgesExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("DeleteEdgesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> deleteEdges();
  Status prepareEdgeKeys(const EdgeType edgeType, const EdgeKeys *edgeKeys);
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_MUTATE_DELETEVERTICESEXECUTOR_H_
