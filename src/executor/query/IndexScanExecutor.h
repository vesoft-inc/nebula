/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_
#define EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "executor/QueryStorageExecutor.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

class IndexScanExecutor final : public QueryStorageExecutor {
public:
    IndexScanExecutor(const PlanNode *node, QueryContext *qctx)
        : QueryStorageExecutor("IndexScanExecutor", node, qctx) {
        gn_ = asNode<IndexScan>(node);
    }

private:
    folly::Future<Status> execute() override;

    folly::Future<Status> indexScan();

    template <typename Resp>
    Status handleResp(storage::StorageRpcResponse<Resp> &&rpcResp);

private:
    const IndexScan *   gn_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_QUERY_INDEXSCANEXECUTOR_H_
