// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_GETEDGESEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_GETEDGESEXECUTOR_H_

#include "graph/executor/query/GetPropExecutor.h"

// get user-specified edge attributes
namespace nebula {
namespace graph {
class GetEdges;
class GetEdgesExecutor final : public GetPropExecutor {
 public:
  GetEdgesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("GetEdgesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  StatusOr<DataSet> buildRequestDataSet(const GetEdges *ge);

  folly::Future<Status> getEdges();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETEDGESEXECUTOR_H_
