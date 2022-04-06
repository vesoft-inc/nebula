// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"
// used in lookup and match scenarios.
// fetch data from storage layer, according to the index selected by the optimizer.
namespace nebula {
namespace graph {

class IndexScanExecutor final : public StorageAccessExecutor {
 public:
  IndexScanExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("IndexScanExecutor", node, qctx) {
    gn_ = asNode<IndexScan>(node);
  }

 private:
  folly::Future<Status> execute() override;

  folly::Future<Status> indexScan();

  template <typename Resp>
  Status handleResp(storage::StorageRpcResponse<Resp> &&rpcResp);

 private:
  const IndexScan *gn_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_
