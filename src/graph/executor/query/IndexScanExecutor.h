/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_

#include "clients/storage/StorageClient.h"
#include "graph/executor/StorageAccessExecutor.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace graph {

class IndexScanExecutor final : public StorageAccessExecutor {
 public:
  IndexScanExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("IndexScanExecutor", node, qctx) {
    gn_ = asNode<IndexScan>(node);
  }

 private:
  folly::Future<Status> execute() override;

  folly::Future<Status> indexScan();

  template <typename Resp>
  Status handleResp(storage::StorageRpcResponse<Resp>&& rpcResp);

 private:
  const IndexScan* gn_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_
