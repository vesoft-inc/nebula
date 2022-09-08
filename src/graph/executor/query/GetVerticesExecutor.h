// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_

#include "graph/executor/query/GetPropExecutor.h"
#include "graph/planner/plan/Query.h"
// get user-specified vertices attributes
namespace nebula {
namespace graph {

class GetVerticesExecutor final : public GetPropExecutor {
 public:
  GetVerticesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("GetVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  StatusOr<DataSet> buildRequestDataSet(const GetVertices *gv);

  folly::Future<Status> getVertices();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_
