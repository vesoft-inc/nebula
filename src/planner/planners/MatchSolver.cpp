/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchSolver.h"

#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

Status MatchSolver::buildReturn(MatchAstContext* mctx, SubPlan& subPlan) {
    auto *yields = new YieldColumns();
    std::vector<std::string> colNames;
    auto *sentence = static_cast<MatchSentence*>(mctx->sentence);
    PlanNode *current = subPlan.root;

    for (auto *col : mctx->yieldColumns->columns()) {
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

    auto *project = Project::make(mctx->qctx, current, yields);
    project->setInputVar(current->outputVar());
    project->setColNames(std::move(colNames));
    current = project;

    if (sentence->ret()->isDistinct()) {
        auto *dedup = Dedup::make(mctx->qctx, current);
        dedup->setInputVar(current->outputVar());
        dedup->setColNames(current->colNames());
        current = dedup;
    }

    if (!mctx->indexedOrderFactors.empty()) {
        auto *sort = Sort::make(mctx->qctx, current, std::move(mctx->indexedOrderFactors));
        sort->setInputVar(current->outputVar());
        sort->setColNames(current->colNames());
        current = sort;
    }

    if (mctx->skip != 0 || mctx->limit != std::numeric_limits<int64_t>::max()) {
        auto *limit = Limit::make(mctx->qctx, current, mctx->skip, mctx->limit);
        limit->setInputVar(current->outputVar());
        limit->setColNames(current->colNames());
        current = limit;
    }

    subPlan.root = current;

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
