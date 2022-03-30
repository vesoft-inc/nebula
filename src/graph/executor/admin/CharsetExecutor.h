// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_CHARSETEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_CHARSETEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ShowCharsetExecutor final : public Executor {
 public:
  ShowCharsetExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowCharsetExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowCollationExecutor final : public Executor {
 public:
  ShowCollationExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowCollationExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_CHARSETEXECUTOR_H_
