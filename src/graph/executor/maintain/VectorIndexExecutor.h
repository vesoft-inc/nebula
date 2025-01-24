// Copyright (c) 2024 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_VECTOR_INDEX_EXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_VECTOR_INDEX_EXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ShowVectorIndexesExecutor final : public Executor {
 public:
  ShowVectorIndexesExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("ShowVectorIndexesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class CreateVectorIndexExecutor final : public Executor {
 public:
  CreateVectorIndexExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("CreateVectorIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DropVectorIndexExecutor final : public Executor {
 public:
  DropVectorIndexExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("DropVectorIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_VECTOR_INDEX_EXECUTOR_H_
