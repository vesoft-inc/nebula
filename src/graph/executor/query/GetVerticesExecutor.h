/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_

#include "graph/executor/query/GetPropExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

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
