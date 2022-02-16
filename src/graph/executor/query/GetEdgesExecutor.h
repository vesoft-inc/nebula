/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_GETEDGESEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETEDGESEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "common/datatypes/DataSet.h"              // for DataSet
#include "graph/executor/query/GetPropExecutor.h"  // for GetPropExecutor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class GetEdges;
class PlanNode;
class QueryContext;

class GetEdgesExecutor final : public GetPropExecutor {
 public:
  GetEdgesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("GetEdgesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  DataSet buildRequestDataSet(const GetEdges *ge);

  folly::Future<Status> getEdges();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETEDGESEXECUTOR_H_
