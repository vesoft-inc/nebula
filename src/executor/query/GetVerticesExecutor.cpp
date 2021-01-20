/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetVerticesExecutor.h"
#include "planner/Query.h"
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
    nebula::DataSet vertices({kVid});
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    if (gv->src() != nullptr) {
        // Accept Table such as | $a | $b | $c |... as input which one column indicate src
        auto valueIter = ectx_->getResult(gv->inputVar()).iter();
        VLOG(1) << "GV input var: " << gv->inputVar() << " iter kind: " << valueIter->kind();
        auto expCtx = QueryExpressionContext(qctx()->ectx());
        for (; valueIter->valid(); valueIter->next()) {
            auto src = gv->src()->eval(expCtx(valueIter.get()));
            VLOG(1) << "src vid: " << src;
            if (!SchemaUtil::isValidVid(src, spaceInfo.spaceDesc.vid_type)) {
                LOG(WARNING) << "Mismatched vid type: " << src.type();
                continue;
            }
            vertices.emplace_back(Row({std::move(src)}));
        }
    }

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
                   &gv->props(),
                   nullptr,
                   gv->exprs().empty() ? nullptr : &gv->exprs(),
                   gv->dedup(),
                   gv->orderBy(),
                   gv->limit(),
                   gv->filter())
        .via(runner())
        .ensure([getPropsTime]() {
            VLOG(1) << "Get props time: " << getPropsTime.elapsedInUSec() << "us";
        })
        .then([this, gv](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            SCOPED_TIMER(&execTime_);
            return handleResp(std::move(rpcResp), gv->colNamesRef());
        });
}

}   // namespace graph
}   // namespace nebula
