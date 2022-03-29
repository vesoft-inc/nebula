// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_APPENDVERTICESEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_APPENDVERTICESEXECUTOR_H_

#include "graph/executor/query/GetPropExecutor.h"
#include "graph/planner/plan/Query.h"
// only used in match scenarios
// due to the architecture design, the attributes of the destination point and the edge are
// not stored together, so the last step in the match statement needs to call this operator
// to obtain the attribute of the destination point
namespace nebula {
namespace graph {

class AppendVerticesExecutor final : public GetPropExecutor {
 public:
  AppendVerticesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("AppendVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  DataSet buildRequestDataSet(const AppendVertices *gv);

  folly::Future<Status> appendVertices();

  folly::Future<Status> handleResp(
      storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp);

  DataSet handleJob(size_t begin, size_t end, Iterator *iter);

  // DstId -> Vertex
  std::unordered_map<Value, Value> dsts_;
  DataSet result_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_GETVERTICESEXECUTOR_H_
