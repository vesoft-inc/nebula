/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_SCANVERTICESEXECUTOR_H
#define GRAPH_EXECUTOR_QUERY_SCANVERTICESEXECUTOR_H

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "graph/executor/query/GetPropExecutor.h"  // for GetPropExecutor
#include "graph/planner/plan/Query.h"

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
