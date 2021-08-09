/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetVerticesExecutor.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetVerticesExecutor::execute() {
    return getVertices();
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
    SCOPED_TIMER(&execTime_);

    auto *gv = asNode<GetVertices>(node());
    GraphStorageClient *storageClient = qctx()->getStorageClient();

    DataSet vertices = buildRequestDataSet(gv);
    VLOG(1) << "vertices: " << vertices;
    if (vertices.rows.empty()) {
        // TODO: add test for empty input.
        return finish(ResultBuilder()
                          .value(Value(DataSet(gv->colNames())))
                          .iter(Iterator::Kind::kProp)
                          .finish());
    }

    time::Duration getPropsTime;
    return DCHECK_NOTNULL(storageClient)
        ->getProps(gv->space(),
                   std::move(vertices),
                   gv->props(),
                   nullptr,
                   gv->exprs(),
                   gv->dedup(),
                   gv->orderBy(),
                   gv->limit(),
                   gv->filter())
        .via(runner())
        .ensure([this, getPropsTime]() {
            SCOPED_TIMER(&execTime_);
            otherStats_.emplace("total_rpc",
                                 folly::stringPrintf("%lu(us)", getPropsTime.elapsedInUSec()));
        })
        .thenValue([this, gv](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            SCOPED_TIMER(&execTime_);
            addStats(rpcResp, otherStats_);
            return handleResp(std::move(rpcResp), gv->colNames());
        });
}

DataSet GetVerticesExecutor::buildRequestDataSet(const GetVertices* gv) {
    if (gv == nullptr) {
        return nebula::DataSet({kVid});
    }
    // Accept Table such as | $a | $b | $c |... as input which one column indicate src
    auto valueIter = ectx_->getResult(gv->inputVar()).iter();
    VLOG(3) << "GV input var: " << gv->inputVar() << " iter kind: " << valueIter->kind();
    return buildRequestDataSetByVidType(valueIter.get(), gv->src(), gv->dedup());
}

}   // namespace graph
}   // namespace nebula
