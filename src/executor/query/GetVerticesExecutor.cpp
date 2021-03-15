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
                   &gv->props(),
                   nullptr,
                   gv->exprs().empty() ? nullptr : &gv->exprs(),
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
        .then([this, gv](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            SCOPED_TIMER(&execTime_);
            addStats(rpcResp, otherStats_);
            return handleResp(std::move(rpcResp), gv->colNamesRef());
        });
}

DataSet GetVerticesExecutor::buildRequestDataSet(const GetVertices* gv) {
    nebula::DataSet vertices({kVid});
    if (gv == nullptr) {
        return vertices;
    }
    // Accept Table such as | $a | $b | $c |... as input which one column indicate src
    auto valueIter = ectx_->getResult(gv->inputVar()).iter();
    VLOG(3) << "GV input var: " << gv->inputVar() << " iter kind: " << valueIter->kind();
    auto expCtx = QueryExpressionContext(qctx()->ectx());
    const auto &spaceInfo = qctx()->rctx()->session()->space();
    vertices.rows.reserve(valueIter->size());
    auto dedup = gv->dedup();
    if (spaceInfo.spaceDesc.vid_type.type == meta::cpp2::PropertyType::INT64) {
        std::unordered_set<int64_t> uniqueSet;
        uniqueSet.reserve(valueIter->size());
        for (; valueIter->valid(); valueIter->next()) {
            auto src = gv->src()->eval(expCtx(valueIter.get()));
            if (!SchemaUtil::isValidVid(src, spaceInfo.spaceDesc.vid_type)) {
                LOG(WARNING) << "Mismatched vid type: " << src.type();
                continue;
            }
            if (dedup && !uniqueSet.emplace(src.getInt()).second) {
                continue;
            }
            vertices.emplace_back(Row({std::move(src)}));
        }
    } else {
        std::unordered_set<std::string> uniqueSet;
        uniqueSet.reserve(valueIter->size());
        for (; valueIter->valid(); valueIter->next()) {
            auto src = gv->src()->eval(expCtx(valueIter.get()));
            if (!SchemaUtil::isValidVid(src, spaceInfo.spaceDesc.vid_type)) {
                LOG(WARNING) << "Mismatched vid type: " << src.type();
                continue;
            }
            if (dedup && !uniqueSet.emplace(src.getStr()).second) {
                continue;
            }
            vertices.emplace_back(Row({std::move(src)}));
        }
    }
    return vertices;
}
}   // namespace graph
}   // namespace nebula
