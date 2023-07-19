// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_SORTEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SORTEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class SortExecutor final : public Executor {
 public:
  SortExecutor(const PlanNode *node, QueryContext *qctx) : Executor("SortExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SORTEXECUTOR_H_
