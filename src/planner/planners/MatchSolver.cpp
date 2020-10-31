/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchSolver.h"

#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

Status MatchSolver::buildReturn(MatchAstContext* matchCtx, SubPlan& subPlan) {
    auto *yields = new YieldColumns();
    std::vector<std::string> colNames;

    for (auto *col : matchCtx->yieldColumns->columns()) {
        auto kind = col->expr()->kind();
        YieldColumn *newColumn = nullptr;
        if (kind == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(label));
        } else if (kind == Expression::Kind::kLabelAttribute) {
            auto *la = static_cast<const LabelAttributeExpression*>(col->expr());
            newColumn = new YieldColumn(rewrite(la));
        } else {
            auto newExpr = col->expr()->clone();
            auto rewriter = [] (const Expression *expr) {
                if (expr->kind() == Expression::Kind::kLabel) {
                    return rewrite(static_cast<const LabelExpression*>(expr));
                } else {
                    return rewrite(static_cast<const LabelAttributeExpression*>(expr));
                }
            };
            RewriteMatchLabelVisitor visitor(std::move(rewriter));
            newExpr->accept(&visitor);
            newColumn = new YieldColumn(newExpr.release());
        }
        yields->addColumn(newColumn);
        if (col->alias() != nullptr) {
            colNames.emplace_back(*col->alias());
        } else {
            colNames.emplace_back(col->expr()->toString());
        }
    }

    auto *project = Project::make(matchCtx->qctx, subPlan.root, yields);
    project->setInputVar(subPlan.root->outputVar());
    project->setColNames(std::move(colNames));
    subPlan.root = project;

    return Status::OK();
}

Expression* MatchSolver::rewrite(const LabelExpression *label) {
    auto *expr = new VariablePropertyExpression(
            new std::string(),
            new std::string(*label->name()));
    return expr;
}

Expression* MatchSolver::rewrite(const LabelAttributeExpression *la) {
    auto *expr = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(),
                new std::string(*la->left()->name())),
            new LabelExpression(*la->right()->name()));
    return expr;
}
}  // namespace graph
}  // namespace nebula
