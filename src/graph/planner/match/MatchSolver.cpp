/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/MatchSolver.h"

#include "common/expression/UnaryExpression.h"
#include "context/ast/AstContext.h"
#include "context/ast/CypherAstContext.h"
#include "planner/Planner.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "util/SchemaUtil.h"
#include "visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
Expression* MatchSolver::rewriteLabel2Vertex(QueryContext* qctx, const Expression* expr) {
    auto* pool = qctx->objPool();
    auto matcher = [](const Expression* e) -> bool {
        return e->kind() == Expression::Kind::kLabel ||
               e->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [&, pool](const Expression* e) -> Expression* {
        DCHECK(e->kind() == Expression::Kind::kLabelAttribute ||
               e->kind() == Expression::Kind::kLabel);
        if (e->kind() == Expression::Kind::kLabelAttribute) {
            auto la = static_cast<const LabelAttributeExpression*>(e);
            return AttributeExpression::make(
                pool, VertexExpression::make(pool), la->right()->clone());
        }
        return VertexExpression::make(pool);
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* MatchSolver::rewriteLabel2Edge(QueryContext* qctx, const Expression* expr) {
    auto* pool = qctx->objPool();
    auto matcher = [](const Expression* e) -> bool {
        return e->kind() == Expression::Kind::kLabel ||
               e->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [&pool](const Expression* e) -> Expression* {
        DCHECK(e->kind() == Expression::Kind::kLabelAttribute ||
               e->kind() == Expression::Kind::kLabel);
        if (e->kind() == Expression::Kind::kLabelAttribute) {
            auto la = static_cast<const LabelAttributeExpression*>(e);
            return AttributeExpression::make(
                pool, EdgeExpression::make(pool), la->right()->clone());
        }
        return EdgeExpression::make(pool);
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* MatchSolver::rewriteLabel2VarProp(QueryContext* qctx, const Expression* expr) {
    auto* pool = qctx->objPool();
    auto matcher = [](const Expression* e) -> bool {
        return e->kind() == Expression::Kind::kLabel ||
               e->kind() == Expression::Kind::kLabelAttribute;
    };
    auto rewriter = [&pool](const Expression* e) -> Expression* {
        DCHECK(e->kind() == Expression::Kind::kLabelAttribute ||
               e->kind() == Expression::Kind::kLabel);
        if (e->kind() == Expression::Kind::kLabelAttribute) {
            auto* la = static_cast<const LabelAttributeExpression*>(e);
            auto* var = VariablePropertyExpression::make(pool, "", la->left()->name());
            return AttributeExpression::make(
                pool, var, ConstantExpression::make(pool, la->right()->value()));
        }
        auto label = static_cast<const LabelExpression*>(e);
        return VariablePropertyExpression::make(pool, "", label->name());
    };

    return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression* MatchSolver::doRewrite(QueryContext* qctx,
                                   const std::unordered_map<std::string, AliasType>& aliases,
                                   const Expression* expr) {
    if (expr->kind() == Expression::Kind::kLabel) {
        auto* labelExpr = static_cast<const LabelExpression*>(expr);
        auto alias = aliases.find(labelExpr->name());
        DCHECK(alias != aliases.end());
    }

    return rewriteLabel2VarProp(qctx, expr);
}

Expression* MatchSolver::makeIndexFilter(const std::string& label,
                                         const MapExpression* map,
                                         QueryContext* qctx,
                                         bool isEdgeProperties) {
    auto makePropExpr = [=, &label](const std::string& prop) -> Expression* {
        if (isEdgeProperties) {
            return EdgePropertyExpression::make(qctx->objPool(), label, prop);
        }
        return TagPropertyExpression::make(qctx->objPool(), label, prop);
    };

    auto root = LogicalExpression::makeAnd(qctx->objPool());
    std::vector<Expression*> operands;
    operands.reserve(map->size());
    for (const auto& item : map->items()) {
        operands.emplace_back(RelationalExpression::makeEQ(
            qctx->objPool(), makePropExpr(item.first), item.second->clone()));
    }
    root->setOperands(std::move(operands));
    return root;
}

Expression* MatchSolver::makeIndexFilter(const std::string& label,
                                         const std::string& alias,
                                         Expression* filter,
                                         QueryContext* qctx,
                                         bool isEdgeProperties) {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kRelEQ,
        Expression::Kind::kRelLT,
        Expression::Kind::kRelLE,
        Expression::Kind::kRelGT,
        Expression::Kind::kRelGE,
    };

    std::vector<const Expression*> ands;
    auto kind = filter->kind();
    if (kinds.count(kind) == 1) {
        ands.emplace_back(filter);
    } else if (kind == Expression::Kind::kLogicalAnd) {
        auto* logic = static_cast<LogicalExpression*>(filter);
        ExpressionUtils::pullAnds(logic);
        for (auto& operand : logic->operands()) {
            ands.emplace_back(operand);
        }
    } else {
        return nullptr;
    }

    auto* pool = qctx->objPool();
    std::vector<Expression*> relationals;
    for (auto* item : ands) {
        if (kinds.count(item->kind()) != 1) {
            continue;
        }

        auto* binary = static_cast<const BinaryExpression*>(item);
        auto* left = binary->left();
        auto* right = binary->right();
        const LabelAttributeExpression* la = nullptr;
        const ConstantExpression* constant = nullptr;
        // TODO(aiee) extract the logic that apllies to both match and lookup
        if (left->kind() == Expression::Kind::kLabelAttribute &&
            right->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(left);
            constant = static_cast<const ConstantExpression*>(right);
        } else if (right->kind() == Expression::Kind::kLabelAttribute &&
                   left->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(right);
            constant = static_cast<const ConstantExpression*>(left);
        } else {
            continue;
        }

        if (la->left()->name() != alias) {
            continue;
        }

        const auto& value = la->right()->value();
        auto* tpExpr = isEdgeProperties ? static_cast<Expression*>(EdgePropertyExpression::make(
                                              pool, label, value.getStr()))
                                        : static_cast<Expression*>(TagPropertyExpression::make(
                                              pool, label, value.getStr()));
        auto* newConstant = constant->clone();
        if (left->kind() == Expression::Kind::kLabelAttribute) {
            auto* rel = RelationalExpression::makeKind(pool, item->kind(), tpExpr, newConstant);
            relationals.emplace_back(rel);
        } else {
            auto* rel = RelationalExpression::makeKind(pool, item->kind(), newConstant, tpExpr);
            relationals.emplace_back(rel);
        }
    }

    if (relationals.empty()) {
        return nullptr;
    }

    auto* root = relationals[0];
    for (auto i = 1u; i < relationals.size(); i++) {
        auto* left = root;
        root = LogicalExpression::makeAnd(qctx->objPool(), left, relationals[i]);
    }

    return root;
}

void MatchSolver::extractAndDedupVidColumn(QueryContext* qctx,
                                           Expression** initialExpr,
                                           PlanNode* dep,
                                           const std::string& inputVar,
                                           SubPlan& plan) {
    auto columns = qctx->objPool()->add(new YieldColumns);
    auto* var = qctx->symTable()->getVar(inputVar);
    Expression* vidExpr = initialExprOrEdgeDstExpr(qctx, initialExpr, var->colNames.back());
    if (initialExpr) {
        *initialExpr = nullptr;
    }
    columns->addColumn(new YieldColumn(vidExpr));
    auto project = Project::make(qctx, dep, columns);
    project->setInputVar(inputVar);
    project->setColNames({kVid});
    auto dedup = Dedup::make(qctx, project);
    dedup->setColNames({kVid});

    plan.root = dedup;
}

Expression* MatchSolver::initialExprOrEdgeDstExpr(QueryContext* qctx,
                                                  Expression** initialExpr,
                                                  const std::string& vidCol) {
    if (initialExpr != nullptr && *initialExpr != nullptr) {
        return *initialExpr;
    } else {
        return getEndVidInPath(qctx, vidCol);
    }
}

Expression* MatchSolver::getEndVidInPath(QueryContext* qctx, const std::string& colName) {
    auto* pool = qctx->objPool();
    // expr: __Project_2[-1] => path
    auto columnExpr = InputPropertyExpression::make(pool, colName);
    // expr: endNode(path) => vn
    auto args = ArgumentList::make(pool);
    args->addArgument(columnExpr);
    auto endNode = FunctionCallExpression::make(pool, "endNode", args);
    // expr: en[_dst] => dst vid
    auto vidExpr = ConstantExpression::make(pool, kVid);
    return AttributeExpression::make(pool, endNode, vidExpr);
}

Expression* MatchSolver::getStartVidInPath(QueryContext *qctx, const std::string& colName) {
    auto* pool = qctx->objPool();
    // expr: __Project_2[0] => path
    auto columnExpr = InputPropertyExpression::make(pool, colName);
    // expr: startNode(path) => v1
    auto args = ArgumentList::make(pool);
    args->addArgument(columnExpr);
    auto firstVertexExpr = FunctionCallExpression::make(pool, "startNode", args);
    // expr: v1[_vid] => vid
    return AttributeExpression::make(pool, firstVertexExpr, ConstantExpression::make(pool, kVid));
}

PlanNode* MatchSolver::filtPathHasSameEdge(PlanNode* input,
                                           const std::string& column,
                                           QueryContext* qctx) {
    auto* pool = qctx->objPool();
    auto args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, column));
    auto fnCall = FunctionCallExpression::make(pool, "hasSameEdgeInPath", args);
    auto cond = UnaryExpression::makeNot(pool, fnCall);
    auto filter = Filter::make(qctx, input, cond);
    filter->setColNames(input->colNames());
    return filter;
}

Status MatchSolver::appendFetchVertexPlan(const Expression* nodeFilter,
                                          const SpaceInfo& space,
                                          QueryContext* qctx,
                                          Expression** initialExpr,
                                          SubPlan& plan) {
    return appendFetchVertexPlan(
        nodeFilter, space, qctx, initialExpr, plan.root->outputVar(), plan);
}

Status MatchSolver::appendFetchVertexPlan(const Expression* nodeFilter,
                                          const SpaceInfo& space,
                                          QueryContext* qctx,
                                          Expression** initialExpr,
                                          std::string inputVar,
                                          SubPlan& plan) {
    auto* pool = qctx->objPool();
    // [Project && Dedup]
    extractAndDedupVidColumn(qctx, initialExpr, plan.root, inputVar, plan);
    auto srcExpr = InputPropertyExpression::make(pool, kVid);
    // [Get vertices]
    auto props = SchemaUtil::getAllVertexProp(qctx, space, true);
    NG_RETURN_IF_ERROR(props);
    auto gv = GetVertices::make(qctx,
                                plan.root,
                                space.id,
                                srcExpr,
                                std::move(props).value(),
                                {});

    PlanNode* root = gv;
    if (nodeFilter != nullptr) {
        auto* newFilter = MatchSolver::rewriteLabel2Vertex(qctx, nodeFilter);
        root = Filter::make(qctx, root, newFilter);
    }

    // Normalize all columns to one
    auto columns = pool->add(new YieldColumns);
    auto pathExpr = PathBuildExpression::make(pool);
    pathExpr->add(VertexExpression::make(pool));
    columns->addColumn(new YieldColumn(pathExpr));
    plan.root = Project::make(qctx, root, columns);
    plan.root->setColNames({kPathStr});
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
