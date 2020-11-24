/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/IndexScanExecutor.h"

#include "planner/PlanNode.h"
#include "context/QueryContext.h"

using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::LookupIndexResp;
using nebula::storage::GraphStorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> IndexScanExecutor::execute() {
    return indexScan();
}

folly::Future<Status> IndexScanExecutor::indexScan() {
    GraphStorageClient* storageClient = qctx_->getStorageClient();
    auto *lookup = asNode<IndexScan>(node());
    return storageClient->lookupIndex(lookup->space(),
                                      *lookup->queryContext(),
                                      lookup->isEdge(),
                                      lookup->schemaId(),
                                      *lookup->returnColumns())
        .via(runner())
        .then([this](StorageRpcResponse<LookupIndexResp> &&rpcResp) {
            return handleResp(std::move(rpcResp));
        });
}

template <typename Resp>
Status IndexScanExecutor::handleResp(storage::StorageRpcResponse<Resp> &&rpcResp) {
    auto completeness = handleCompleteness(rpcResp, false);
    if (!completeness.ok()) {
        return std::move(completeness).status();
    }
    auto state = std::move(completeness).value();
    nebula::DataSet v;
    for (auto &resp : rpcResp.responses()) {
        if (resp.__isset.data) {
            nebula::DataSet* data = resp.get_data();
            // TODO : convert the column name to alias.
            if (v.colNames.empty()) {
                v.colNames = data->colNames;
            }
            v.rows.insert(v.rows.end(), data->rows.begin(), data->rows.end());
        } else {
            state = Result::State::kPartialSuccess;
        }
    }
    // TODO(yee): Unify the response structure of IndexScan and GetProps and change the following
    // iterator to PropIter type
    VLOG(2) << "Dataset produced by IndexScan: \n" << v << "\n";
    return finish(ResultBuilder()
                      .value(std::move(v))
                      .iter(Iterator::Kind::kSequential)
                      .state(state)
                      .finish());
}

}   // namespace graph
}   // namespace nebula
