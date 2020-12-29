/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/PropIndexSeek.h"

#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
bool PropIndexSeek::matchEdge(EdgeContext* edgeCtx) {
    UNUSED(edgeCtx);
    return false;
}

StatusOr<SubPlan> PropIndexSeek::transformEdge(EdgeContext* edgeCtx) {
    UNUSED(edgeCtx);
    return Status::Error("Unimplement for edge pattern.");
}

bool PropIndexSeek::matchNode(NodeContext* nodeCtx) {
    auto& node = *nodeCtx->info;
    if (node.label == nullptr) {
        return false;
    }

    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    Expression* filter = nullptr;
    if (matchClauseCtx->where != nullptr
            && matchClauseCtx->where->filter != nullptr) {
        filter = MatchSolver::makeIndexFilter(
            *node.label, *node.alias, matchClauseCtx->where->filter.get(), matchClauseCtx->qctx);
    }
    if (filter == nullptr) {
        if (node.props != nullptr && !node.props->items().empty()) {
            filter = MatchSolver::makeIndexFilter(*node.label, node.props, matchClauseCtx->qctx);
        }
    }

    if (filter == nullptr) {
        return false;
    }

    nodeCtx->scanInfo.filter = filter;
    nodeCtx->scanInfo.schemaId = node.tid;
    nodeCtx->scanInfo.schemaName = node.label;

    return true;
}

StatusOr<SubPlan> PropIndexSeek::transformNode(NodeContext* nodeCtx) {
    SubPlan plan;
    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    using IQC = nebula::storage::cpp2::IndexQueryContext;
    IQC iqctx;
    iqctx.set_filter(Expression::encode(*nodeCtx->scanInfo.filter));
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back(std::move(iqctx));
    auto columns = std::make_unique<std::vector<std::string>>();
    columns->emplace_back(kVid);
    auto scan = IndexScan::make(matchClauseCtx->qctx,
                                nullptr,
                                matchClauseCtx->space.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                nodeCtx->scanInfo.schemaId);
    scan->setColNames({kVid});
    plan.tail = scan;
    plan.root = scan;

    // initialize start expression in project node
    nodeCtx->initialExpr = std::unique_ptr<Expression>(ExpressionUtils::newVarPropExpr(kVid));
    return plan;
}

}  // namespace graph
}  // namespace nebula
