/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_APPENDVERTICESEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_APPENDVERTICESEXECUTOR_H_

#include "graph/executor/query/GetPropExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

class AppendVerticesExecutor final : public GetPropExecutor {
 public:
  AppendVerticesExecutor(const PlanNode* node, QueryContext* qctx)
      : GetPropExecutor("AppendVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  DataSet buildRequestDataSet(const AppendVertices* gv);

  folly::Future<Status> appendVertices();

  Status handleResp(storage::StorageRpcResponse<storage::cpp2::GetPropResponse>&& rpcResp);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_
