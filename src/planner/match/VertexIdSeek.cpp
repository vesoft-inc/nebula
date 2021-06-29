/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/VertexIdSeek.h"

#include "planner/plan/Logic.h"
#include "planner/match/MatchSolver.h"
#include "visitor/VidExtractVisitor.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {
bool VertexIdSeek::matchEdge(EdgeContext *edgeCtx) {
    UNUSED(edgeCtx);
    return false;
}

StatusOr<SubPlan> VertexIdSeek::transformEdge(EdgeContext *edgeCtx) {
    UNUSED(edgeCtx);
    return Status::Error("Unimplemented for edge pattern.");
}

bool VertexIdSeek::matchNode(NodeContext *nodeCtx) {
    auto &node = *nodeCtx->info;
    auto *matchClauseCtx = nodeCtx->matchClauseCtx;
    if (matchClauseCtx->where == nullptr || matchClauseCtx->where->filter == nullptr) {
        return false;
    }

    if (node.alias.empty() || node.anonymous) {
        // require one named node
        return false;
    }

    VidExtractVisitor vidExtractVisitor;
    matchClauseCtx->where->filter->accept(&vidExtractVisitor);
    auto vidResult = vidExtractVisitor.moveVidPattern();
    if (vidResult.spec != VidExtractVisitor::VidPattern::Special::kInUsed) {
        return false;
    }
    for (auto &nodeVid : vidResult.nodes) {
        if (nodeVid.second.kind == VidExtractVisitor::VidPattern::Vids::Kind::kIn) {
            if (nodeVid.first == node.alias) {
                nodeCtx->ids = std::move(nodeVid.second.vids);
                return true;
            }
        }
    }

    return false;
}

std::pair<std::string, Expression *> VertexIdSeek::listToAnnoVarVid(QueryContext *qctx,
                                                                    const List &list) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    for (auto &v : list.values) {
        vids.emplace_back(Row({std::move(v)}));
    }

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto* pool = qctx->objPool();
    auto *src = VariablePropertyExpression::make(pool, input, kVid);
    return std::pair<std::string, Expression *>(input, src);
}

std::pair<std::string, Expression *> VertexIdSeek::constToAnnoVarVid(QueryContext *qctx,
                                                                     const Value &v) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    vids.emplace_back(Row({v}));

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto* pool = qctx->objPool();
    auto *src = VariablePropertyExpression::make(pool, input, kVid);
    return std::pair<std::string, Expression *>(input, src);
}

StatusOr<SubPlan> VertexIdSeek::transformNode(NodeContext *nodeCtx) {
    SubPlan plan;
    auto *matchClauseCtx = nodeCtx->matchClauseCtx;
    auto *qctx = matchClauseCtx->qctx;

    QueryExpressionContext dummy;
    std::pair<std::string, Expression *> vidsResult = listToAnnoVarVid(qctx, nodeCtx->ids);

    auto* passThrough = PassThroughNode::make(qctx, nullptr);
    passThrough->setOutputVar(vidsResult.first);
    passThrough->setColNames({kVid});
    plan.root = passThrough;
    plan.tail = passThrough;

    nodeCtx->initialExpr = vidsResult.second;
    return plan;
}

}   // namespace graph
}   // namespace nebula
