/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "common/datatypes/DataSet.h"              // for DataSet
#include "graph/executor/query/GetPropExecutor.h"  // for GetPropExecutor
#include "graph/planner/plan/Query.h"

namespace nebula {
class Status;
namespace graph {
class GetVertices;
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class GetVertices;
class PlanNode;
class QueryContext;

class GetVerticesExecutor final : public GetPropExecutor {
 public:
  GetVerticesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("GetVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  DataSet buildRequestDataSet(const GetVertices *gv);

  folly::Future<Status> getVertices();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_
