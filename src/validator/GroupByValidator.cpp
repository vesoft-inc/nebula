/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"
#include "planner/Query.h"
#include "util/AnonColGenerator.h"
#include "util/AnonVarGenerator.h"
#include "util/ExpressionUtils.h"
#include "visitor/FindAnySubExprVisitor.h"

namespace nebula {
namespace graph {

Status GroupByValidator::validateImpl() {
    auto* groupBySentence = static_cast<GroupBySentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateGroup(groupBySentence->groupClause()));
    NG_RETURN_IF_ERROR(validateYield(groupBySentence->yieldClause()));
    NG_RETURN_IF_ERROR(groupClauseSemanticCheck());

    return Status::OK();
}

Status GroupByValidator::validateYield(const YieldClause* yieldClause) {
    std::vector<YieldColumn*> columns;
    if (yieldClause != nullptr) {
        columns = yieldClause->columns();
    }
    if (columns.empty()) {
        return Status::SemanticError("Yield cols is Empty");
    }

    projCols_ = qctx_->objPool()->add(new YieldColumns);
    for (auto* col : columns) {
        auto rewrited = false;
        auto colOldName = deduceColName(col);
        if (col->expr()->kind() != Expression::Kind::kAggregate) {
            NG_RETURN_IF_ERROR(rewriteInnerAggExpr(col, rewrited));
        }
        auto colExpr = col->expr();
        // collect exprs for check later
        if (colExpr->kind() == Expression::Kind::kAggregate) {
            auto* aggExpr = static_cast<AggregateExpression*>(colExpr);
            NG_RETURN_IF_ERROR(checkAggExpr(aggExpr));
        } else {
            yieldCols_.emplace_back(colExpr);
        }

        groupItems_.emplace_back(colExpr);
        auto status = deduceExprType(colExpr);
        NG_RETURN_IF_ERROR(status);
        auto type = std::move(status).value();
        std::string name;
        if (!rewrited) {
            name = deduceColName(col);
            projCols_->addColumn(new YieldColumn(
                new VariablePropertyExpression(new std::string(), new std::string(name)),
                new std::string(colOldName)));
        } else {
            name = colExpr->toString();
        }
        projOutputColumnNames_.emplace_back(colOldName);
        outputs_.emplace_back(name, type);
        outputColumnNames_.emplace_back(name);

        if (col->alias() != nullptr && !rewrited) {
            aliases_.emplace(*col->alias(), col);
        }

        ExpressionProps yieldProps;
        NG_RETURN_IF_ERROR(deduceProps(colExpr, yieldProps));
        exprProps_.unionProps(std::move(yieldProps));
    }

    return Status::OK();
}

Status GroupByValidator::validateGroup(const GroupClause* groupClause) {
    if (!groupClause) return Status::OK();
    std::vector<YieldColumn*> columns;
    if (groupClause != nullptr) {
        columns = groupClause->columns();
    }

    auto groupByValid = [](Expression::Kind kind) -> bool {
        return std::unordered_set<Expression::Kind>{Expression::Kind::kAdd,
                                                    Expression::Kind::kMinus,
                                                    Expression::Kind::kMultiply,
                                                    Expression::Kind::kDivision,
                                                    Expression::Kind::kMod,
                                                    Expression::Kind::kTypeCasting,
                                                    Expression::Kind::kFunctionCall,
                                                    Expression::Kind::kInputProperty,
                                                    Expression::Kind::kVarProperty,
                                                    Expression::Kind::kCase}
            .count(kind);
    };
    for (auto* col : columns) {
        if (graph::ExpressionUtils::findAny(col->expr(), {Expression::Kind::kAggregate}) ||
            !graph::ExpressionUtils::findAny(
                col->expr(), {Expression::Kind::kInputProperty, Expression::Kind::kVarProperty})) {
            return Status::SemanticError("Group `%s' invalid", col->expr()->toString().c_str());
        }
        if (!groupByValid(col->expr()->kind())) {
            return Status::SemanticError("Group `%s' invalid", col->expr()->toString().c_str());
        }

        NG_RETURN_IF_ERROR(deduceExprType(col->expr()));
        NG_RETURN_IF_ERROR(deduceProps(col->expr(), exprProps_));

        groupKeys_.emplace_back(col->expr());
    }
    return Status::OK();
}

Status GroupByValidator::toPlan() {
    auto* groupBy = Aggregate::make(qctx_, nullptr, std::move(groupKeys_), std::move(groupItems_));
    groupBy->setColNames(std::vector<std::string>(outputColumnNames_));
    if (needGenProject_) {
        // rewrite Expr which has inner aggExpr and push it up to Project.
        auto* project = Project::make(qctx_, groupBy, projCols_);
        project->setInputVar(groupBy->outputVar());
        project->setColNames(projOutputColumnNames_);
        root_ = project;
    } else {
        root_ = groupBy;
    }
    tail_ = groupBy;
    return Status::OK();
}

Status GroupByValidator::groupClauseSemanticCheck() {
    // check exprProps
    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
        return Status::SemanticError("Only support input and variable in GroupBy sentence.");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable in GroupBy sentence.");
    }
    if (!exprProps_.varProps().empty() && exprProps_.varProps().size() > 1) {
        return Status::SemanticError("Only one variable allowed to use.");
    }

    if (groupKeys_.empty()) {
        groupKeys_ = yieldCols_;
    } else {
        std::unordered_set<Expression*> groupSet(groupKeys_.begin(), groupKeys_.end());
        for (auto* expr : yieldCols_) {
            if (evaluableExpr(expr)) {
                continue;
            }
            FindAnySubExprVisitor groupVisitor(groupSet, true);
            expr->accept(&groupVisitor);
            if (!groupVisitor.found()) {
                return Status::SemanticError("Yield non-agg expression `%s' must be"
                                             " functionally dependent on items in GROUP BY clause",
                                             expr->toString().c_str());
            }
        }
    }
    return Status::OK();
}

Status GroupByValidator::rewriteInnerAggExpr(YieldColumn* col, bool& rewrited) {
    auto colOldName = deduceColName(col);
    auto* colExpr = col->expr();
    // agg col need not rewrite
    DCHECK(colExpr->kind() != Expression::Kind::kAggregate);
    auto collectAggCol = colExpr->clone();
    auto aggs = ExpressionUtils::collectAll(collectAggCol.get(), {Expression::Kind::kAggregate});
    if (aggs.size() > 1) {
        return Status::SemanticError("Aggregate function nesting is not allowed: `%s'",
                                     collectAggCol->toString().c_str());
    }
    if (aggs.size() == 1) {
        auto* colRewrited = ExpressionUtils::rewriteAgg2VarProp(colExpr);
        rewrited = true;
        needGenProject_ = true;
        projCols_->addColumn(new YieldColumn(colRewrited, new std::string(colOldName)));

        // set aggExpr
        col->setExpr(aggs[0]->clone().release());
    }

    return Status::OK();
}

Status GroupByValidator::checkAggExpr(AggregateExpression* aggExpr) {
    auto func = *aggExpr->name();
    std::transform(func.begin(), func.end(), func.begin(), ::toupper);
    NG_RETURN_IF_ERROR(AggFunctionManager::find(func));

    auto* aggArg = aggExpr->arg();
    if (graph::ExpressionUtils::findAny(aggArg, {Expression::Kind::kAggregate})) {
        return Status::SemanticError("Aggregate function nesting is not allowed: `%s'",
                                     aggExpr->toString().c_str());
    }

    if (func.compare("COUNT")) {
        if (aggArg->toString() == "*") {
            return Status::SemanticError("Could not apply aggregation function `%s' on `*`",
                                         aggExpr->toString().c_str());
        }
        if (aggArg->kind() == Expression::Kind::kInputProperty ||
            aggArg->kind() == Expression::Kind::kVarProperty) {
            auto propExpr = static_cast<PropertyExpression*>(aggArg);
            if (*propExpr->prop() == "*") {
                return Status::SemanticError("Could not apply aggregation function `%s' on `%s'",
                                             aggExpr->toString().c_str(),
                                             propExpr->toString().c_str());
            }
        }
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
