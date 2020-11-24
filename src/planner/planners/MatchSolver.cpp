/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchSolver.h"

#include "context/AstContext.h"
#include "util/ExpressionUtils.h"
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
        auto rewriter = [mctx](const Expression *expr) {
            return MatchSolver::doRewrite(mctx, expr);
        };
        if (kind == Expression::Kind::kLabel || kind == Expression::Kind::kLabelAttribute) {
            newColumn = new YieldColumn(rewriter(col->expr()));
        } else {
            auto newExpr = col->expr()->clone();
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

Expression *MatchSolver::doRewrite(const MatchAstContext *mctx, const Expression *expr) {
    if (expr->kind() != Expression::Kind::kLabel) {
        return rewrite(static_cast<const LabelAttributeExpression *>(expr));
    }

    auto *labelExpr = static_cast<const LabelExpression *>(expr);
    auto alias = mctx->aliases.find(*labelExpr->name());
    DCHECK(alias != mctx->aliases.end());
    if (false /* alias->second == MatchValidator::AliasType::kPath */) {
        return mctx->pathBuild->clone().release();
    } else {
        return rewrite(labelExpr);
    }
}

bool MatchSolver::match(AstContext *astCtx) {
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return false;
    }
    auto *matchCtx = static_cast<MatchAstContext *>(astCtx);

    auto &head = matchCtx->nodeInfos[0];
    if (head.label == nullptr) {
        return false;
    }

    Expression *filter = nullptr;
    if (matchCtx->filter != nullptr) {
        filter = MatchSolver::makeIndexFilter(
            *head.label, *head.alias, matchCtx->filter.get(), matchCtx->qctx);
    }
    if (filter == nullptr) {
        if (head.props != nullptr && !head.props->items().empty()) {
            filter = MatchSolver::makeIndexFilter(*head.label, head.props, matchCtx->qctx);
        }
    }

    if (filter == nullptr) {
        return false;
    }

    matchCtx->scanInfo.filter = filter;
    matchCtx->scanInfo.schemaId = head.tid;

    return true;
}

Expression *MatchSolver::makeIndexFilter(const std::string &label,
                                         const MapExpression *map,
                                         QueryContext *qctx) {
    auto &items = map->items();
    Expression *root = new RelationalExpression(
        Expression::Kind::kRelEQ,
        new TagPropertyExpression(new std::string(label), new std::string(*items[0].first)),
        items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        auto *left = root;
        auto *right = new RelationalExpression(
            Expression::Kind::kRelEQ,
            new TagPropertyExpression(new std::string(label), new std::string(*items[i].first)),
            items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }
    return qctx->objPool()->add(root);
}

Expression *MatchSolver::makeIndexFilter(const std::string &label,
                                         const std::string &alias,
                                         Expression *filter,
                                         QueryContext *qctx) {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kRelEQ,
        Expression::Kind::kRelLT,
        Expression::Kind::kRelLE,
        Expression::Kind::kRelGT,
        Expression::Kind::kRelGE,
    };

    std::vector<const Expression *> ands;
    auto kind = filter->kind();
    if (kinds.count(kind) == 1) {
        ands.emplace_back(filter);
    } else if (kind == Expression::Kind::kLogicalAnd) {
        auto *logic = static_cast<LogicalExpression *>(filter);
        ExpressionUtils::pullAnds(logic);
        for (auto &operand : logic->operands()) {
            ands.emplace_back(operand.get());
        }
    } else {
        return nullptr;
    }

    std::vector<Expression *> relationals;
    for (auto *item : ands) {
        if (kinds.count(item->kind()) != 1) {
            continue;
        }

        auto *binary = static_cast<const BinaryExpression *>(item);
        auto *left = binary->left();
        auto *right = binary->right();
        const LabelAttributeExpression *la = nullptr;
        const ConstantExpression *constant = nullptr;
        if (left->kind() == Expression::Kind::kLabelAttribute &&
            right->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression *>(left);
            constant = static_cast<const ConstantExpression *>(right);
        } else if (right->kind() == Expression::Kind::kLabelAttribute &&
                   left->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression *>(right);
            constant = static_cast<const ConstantExpression *>(left);
        } else {
            continue;
        }

        if (*la->left()->name() != alias) {
            continue;
        }

        auto *tpExpr = new TagPropertyExpression(new std::string(label),
                                                 new std::string(*la->right()->name()));
        auto *newConstant = constant->clone().release();
        if (left->kind() == Expression::Kind::kLabelAttribute) {
            auto *rel = new RelationalExpression(item->kind(), tpExpr, newConstant);
            relationals.emplace_back(rel);
        } else {
            auto *rel = new RelationalExpression(item->kind(), newConstant, tpExpr);
            relationals.emplace_back(rel);
        }
    }

    if (relationals.empty()) {
        return nullptr;
    }

    auto *root = relationals[0];
    for (auto i = 1u; i < relationals.size(); i++) {
        auto *left = root;
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, relationals[i]);
    }

    return qctx->objPool()->add(root);
}

Status MatchSolver::buildFilter(const MatchAstContext *mctx, SubPlan *plan) {
    if (mctx->filter == nullptr) {
        return Status::OK();
    }
    auto newFilter = mctx->filter->clone();
    auto rewriter = [mctx](const Expression *expr) { return MatchSolver::doRewrite(mctx, expr); };
    RewriteMatchLabelVisitor visitor(std::move(rewriter));
    newFilter->accept(&visitor);
    auto cond = mctx->qctx->objPool()->add(newFilter.release());
    auto input = plan->root;
    auto *node = Filter::make(mctx->qctx, input, cond);
    node->setInputVar(input->outputVar());
    node->setColNames(input->colNames());
    plan->root = node;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
