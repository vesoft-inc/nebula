/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/VertexIdSeek.h"

#include "planner/Logic.h"
#include "planner/match/MatchSolver.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
bool VertexIdSeek::matchEdge(EdgeContext* edgeCtx) {
    UNUSED(edgeCtx);
    return false;
}

StatusOr<SubPlan> VertexIdSeek::transformEdge(EdgeContext* edgeCtx) {
    UNUSED(edgeCtx);
    return Status::Error("Unimplement for edge pattern.");
}

bool VertexIdSeek::matchNode(NodeContext* nodeCtx) {
    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    auto where = matchClauseCtx->where.get();
    if (where == nullptr || where->filter == nullptr) {
        return false;
    }

    DCHECK(nodeCtx->info != nullptr);
    auto alias = nodeCtx->info->alias;
    if (alias == nullptr) {
        return false;
    }

    auto vidResult = extractVids(*alias, where->filter.get());
    if (!vidResult.ok()) {
        return false;
    }

    nodeCtx->ids = vidResult.value();
    return true;
}

static Status checkIDFun(const std::string& alias, const FunctionCallExpression* fCallExpr) {
    if (*fCallExpr->name() != "id") {
        return Status::Error("Only id function is allowed");
    }
    const auto& args = DCHECK_NOTNULL(fCallExpr->args())->args();
    if (args.size() != 1U) {
        return Status::Error("Invalid parameter number of id function");
    }
    auto arg = args.back().get();
    if (arg->toString() != alias) {
        return Status::Error("Invalid parameter `%s' of id function", alias.c_str());
    }
    return Status::OK();
}

StatusOr<const Expression*> VertexIdSeek::extractVids(const std::string& alias,
                                                      const Expression* filter) {
    if (filter->kind() == Expression::Kind::kRelIn) {
        const auto* inExpr = static_cast<const RelationalExpression*>(filter);
        if (inExpr->left()->kind() != Expression::Kind::kFunctionCall ||
            inExpr->right()->kind() != Expression::Kind::kConstant) {
            return Status::Error("Not supported expression.");
        }
        const auto* fCallExpr = static_cast<const FunctionCallExpression*>(inExpr->left());
        NG_RETURN_IF_ERROR(checkIDFun(alias, fCallExpr));
        auto* constExpr = const_cast<Expression*>(inExpr->right());
        return constExpr;
    } else if (filter->kind() == Expression::Kind::kRelEQ) {
        const auto* eqExpr = static_cast<const RelationalExpression*>(filter);
        if (eqExpr->left()->kind() != Expression::Kind::kFunctionCall ||
            eqExpr->right()->kind() != Expression::Kind::kConstant) {
            return Status::Error("Not supported expression.");
        }
        const auto* fCallExpr = static_cast<const FunctionCallExpression*>(eqExpr->left());
        NG_RETURN_IF_ERROR(checkIDFun(alias, fCallExpr));
        auto* constExpr = const_cast<Expression*>(eqExpr->right());
        return constExpr;
    } else {
        return Status::Error("Not supported expression.");
    }
}

std::pair<std::string, Expression*> VertexIdSeek::listToAnnoVarVid(QueryContext* qctx,
                                                                    const List& list) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    for (auto& v : list.values) {
        vids.emplace_back(Row({std::move(v)}));
    }

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto* src = new VariablePropertyExpression(new std::string(input), new std::string(kVid));
    return std::pair<std::string, Expression*>(input, src);
}

std::pair<std::string, Expression*> VertexIdSeek::constToAnnoVarVid(QueryContext* qctx,
                                                                     const Value& v) {
    auto input = qctx->vctx()->anonVarGen()->getVar();
    DataSet vids({kVid});
    QueryExpressionContext dummy;
    vids.emplace_back(Row({v}));

    qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).finish());

    auto* src = new VariablePropertyExpression(new std::string(input), new std::string(kVid));
    return std::pair<std::string, Expression*>(input, src);
}

StatusOr<SubPlan> VertexIdSeek::transformNode(NodeContext* nodeCtx) {
    SubPlan plan;
    auto* matchClauseCtx = nodeCtx->matchClauseCtx;
    auto* qctx = matchClauseCtx->qctx;

    QueryExpressionContext dummy;
    const auto& value = const_cast<Expression*>(nodeCtx->ids)->eval(dummy);
    std::pair<std::string, Expression*> vidsResult;
    if (value.isList()) {
        vidsResult = listToAnnoVarVid(qctx, value.getList());
    } else {
        vidsResult = constToAnnoVarVid(qctx, value);
    }

    auto* passThrough = PassThroughNode::make(qctx, nullptr);
    passThrough->setOutputVar(vidsResult.first);
    passThrough->setColNames({kVid});
    plan.root = passThrough;
    plan.tail = passThrough;

    nodeCtx->initialExpr = std::unique_ptr<Expression>(vidsResult.second);
    return plan;
}
}  // namespace graph
}  // namespace nebula
