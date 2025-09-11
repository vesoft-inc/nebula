// Copyright (c) 2025 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_ANNINDEXSCANEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_ANNINDEXSCANEXECUTOR_H_

#include <bits/stdint-intn.h>

#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"
// used in lookup and match scenarios.
// fetch data from storage layer, according to the index selected by the optimizer.
namespace nebula {
namespace graph {

class AnnIndexScanExecutor final : public StorageAccessExecutor {
 public:
  AnnIndexScanExecutor(const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor("AnnIndexScanExecutor", node, qctx) {
    gn_ = asNode<AnnIndexScan>(node);
  }

 private:
  folly::Future<Status> execute() override;

  folly::Future<Status> annIndexScan();

  template <typename Resp>
  Status handleResp(storage::StorageRpcResponse<Resp> &&rpcResp);

 private:
  const AnnIndexScan *gn_;
  static constexpr int64_t K = 3;  // we always get K times of limit from storage layer
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_
