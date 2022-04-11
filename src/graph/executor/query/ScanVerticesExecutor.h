// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_SCANVERTICESEXECUTOR_H
#define GRAPH_EXECUTOR_QUERY_SCANVERTICESEXECUTOR_H

#include "graph/executor/query/GetPropExecutor.h"
// full table scan vertices, but must have limit parameter
namespace nebula {
namespace graph {

class ScanVerticesExecutor final : public GetPropExecutor {
 public:
  ScanVerticesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("ScanVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> scanVertices();
};

}  // namespace graph
}  // namespace nebula
#endif
