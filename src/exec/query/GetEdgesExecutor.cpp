/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetEdgesExecutor.h"
#include "planner/Query.h"
#include "context/QueryContext.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
    return getEdges();
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
    dumpLog();

    GraphStorageClient *client = qctx()->getStorageClient();

    auto *ge = asNode<GetEdges>(node());
    nebula::DataSet edges({kSrc, kType, kRank, kDst});
    if (!ge->edges().empty()) {
        edges.rows.insert(edges.rows.end(),
                          std::make_move_iterator(ge->edges().begin()),
                          std::make_move_iterator(ge->edges().end()));
    }
    if (ge->src() != nullptr && ge->ranking() != nullptr && ge->dst() != nullptr) {
        // Accept Table such as | $a | $b | $c | $d |... which indicate src, ranking or dst
        auto valueIter = ectx_->getResult(ge->inputVar()).iter();
        auto expCtx = QueryExpressionContext(qctx()->ectx(), valueIter.get());
        for (; valueIter->valid(); valueIter->next()) {
            auto src = ge->src()->eval(expCtx);
            auto ranking = ge->ranking()->eval(expCtx);
            auto dst = ge->dst()->eval(expCtx);
            if (!src.isStr() || !ranking.isInt() || !dst.isStr()) {
                LOG(WARNING) << "Mismatched edge key type";
                continue;
            }
            edges.emplace_back(Row({
                std::move(src), ge->type(), std::move(ranking), std::move(dst)
            }));
        }
    }
    return DCHECK_NOTNULL(client)
        ->getProps(ge->space(),
                   std::move(edges),
                   nullptr,
                   &ge->props(),
                   ge->exprs().empty() ? nullptr : &ge->exprs(),
                   ge->dedup(),
                   ge->orderBy(),
                   ge->limit(),
                   ge->filter())
        .via(runner())
        .then([this](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            return handleResp(std::move(rpcResp));
        });
}

}   // namespace graph
}   // namespace nebula
