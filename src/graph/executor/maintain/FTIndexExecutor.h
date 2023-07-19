// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_SHOW_FT_INDEXES_EXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SHOW_FT_INDEXES_EXECUTOR_H_

#include "graph/executor/Executor.h"
// full-text indexes are used to do "query_string" search on a string property.
// you can use the WHERE clause to specify the search strings in LOOKUP statements.
namespace nebula {
namespace graph {

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
