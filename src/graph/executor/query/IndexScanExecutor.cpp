/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/IndexScanExecutor.h"

#include <algorithm>

#include "planner/plan/PlanNode.h"
#include "context/QueryContext.h"
#include "service/GraphFlags.h"

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
    if (lookup->isEmptyResultSet()) {
        DataSet dataSet({"dummy"});
        return finish(ResultBuilder().value(Value(std::move(dataSet))).finish());
    }

    const auto &ictxs = lookup->queryContext();
    auto iter = std::find_if(
        ictxs.begin(), ictxs.end(), [](auto &ictx) { return !ictx.index_id_ref().is_set(); });
    if (ictxs.empty() || iter != ictxs.end()) {
        return Status::Error("There is no index to use at runtime");
    }

    return storageClient->lookupIndex(lookup->space(),
                                      ictxs,
                                      lookup->isEdge(),
                                      lookup->schemaId(),
                                      lookup->returnColumns())
        .via(runner())
        .thenValue([this](StorageRpcResponse<LookupIndexResp> &&rpcResp) {
            return handleResp(std::move(rpcResp));
        });
}

// TODO(shylock) merge the handler with GetProp
template <typename Resp>
Status IndexScanExecutor::handleResp(storage::StorageRpcResponse<Resp> &&rpcResp) {
    auto completeness = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
    if (!completeness.ok()) {
        return std::move(completeness).status();
    }
    auto state = std::move(completeness).value();
    nebula::DataSet v;
    for (auto &resp : rpcResp.responses()) {
        if (resp.data_ref().has_value()) {
            nebula::DataSet& data = *resp.data_ref();
            // TODO : convert the column name to alias.
            if (v.colNames.empty()) {
                v.colNames = data.colNames;
            }
            v.rows.insert(v.rows.end(), data.rows.begin(), data.rows.end());
        } else {
            state = Result::State::kPartialSuccess;
        }
    }
    if (!node()->colNames().empty()) {
        DCHECK_EQ(node()->colNames().size(), v.colNames.size());
        v.colNames = node()->colNames();
    }
    VLOG(2) << "Dataset produced by IndexScan: \n" << v << "\n";
    return finish(ResultBuilder()
                      .value(std::move(v))
                      .iter(Iterator::Kind::kProp)
                      .state(state)
                      .finish());
}

}   // namespace graph
}   // namespace nebula
