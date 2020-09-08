/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
#define EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_

#include <vector>

#include "common/base/StatusOr.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/clients/storage/GraphStorageClient.h"

#include "executor/QueryStorageExecutor.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
class GetNeighborsExecutor final : public QueryStorageExecutor {
public:
    GetNeighborsExecutor(const PlanNode *node, QueryContext *qctx)
        : QueryStorageExecutor("GetNeighborsExecutor", node, qctx) {
        gn_ = asNode<GetNeighbors>(node);
    }

    folly::Future<Status> execute() override;

    Status close() override;

private:
    friend class GetNeighborsTest_BuildRequestDataSet_Test;
    Status buildRequestDataSet();

    folly::Future<Status> getNeighbors();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsResponse>;
    Status handleResponse(RpcResponse& resps);

private:
    DataSet               reqDs_;
    const GetNeighbors*   gn_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_QUERY_GETNEIGHBORSEXECUTOR_H_
