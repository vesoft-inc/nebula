/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/GetEdgesExecutor.h"
#include "context/QueryContext.h"
#include "planner/plan/Query.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetEdgesExecutor::execute() {
    return getEdges();
}

folly::Future<Status> GetEdgesExecutor::getEdges() {
    SCOPED_TIMER(&execTime_);

    GraphStorageClient *client = qctx()->getStorageClient();

    auto *ge = asNode<GetEdges>(node());
    nebula::DataSet edges({kSrc, kType, kRank, kDst});
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    if (ge->src() != nullptr &&
        ge->type() != nullptr &&
        ge->ranking() != nullptr &&
        ge->dst() != nullptr) {
        // Accept Table such as | $a | $b | $c | $d |... which indicate src, ranking or dst
        auto valueIter = ectx_->getResult(ge->inputVar()).iter();
        auto expCtx = QueryExpressionContext(qctx()->ectx());
        for (; valueIter->valid(); valueIter->next()) {
            auto src = ge->src()->eval(expCtx(valueIter.get()));
            auto type = ge->type()->eval(expCtx(valueIter.get()));
            auto ranking = ge->ranking()->eval(expCtx(valueIter.get()));
            auto dst = ge->dst()->eval(expCtx(valueIter.get()));
            if (!SchemaUtil::isValidVid(src, *spaceInfo.spaceDesc.vid_type_ref())
                    || !SchemaUtil::isValidVid(dst, *spaceInfo.spaceDesc.vid_type_ref())
                    || !type.isInt() || !ranking.isInt()) {
                LOG(WARNING) << "Mismatched edge key type";
                continue;
            }
            edges.emplace_back(Row({
                std::move(src), type, ranking, std::move(dst)
            }));
        }
    }

    if (edges.rows.empty()) {
        // TODO: add test for empty input.
        return finish(ResultBuilder()
                          .value(Value(DataSet(ge->colNames())))
                          .iter(Iterator::Kind::kProp)
                          .finish());
    }

    time::Duration getPropsTime;
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
        .ensure([this, getPropsTime]() {
            SCOPED_TIMER(&execTime_);
            otherStats_.emplace("total_rpc",
                                folly::stringPrintf("%lu(us)", getPropsTime.elapsedInUSec()));
        })
        .thenValue([this, ge](StorageRpcResponse<GetPropResponse> &&rpcResp) {
            SCOPED_TIMER(&execTime_);
            addStats(rpcResp, otherStats_);
            return handleResp(std::move(rpcResp), ge->colNamesRef());
        });
}

}   // namespace graph
}   // namespace nebula
