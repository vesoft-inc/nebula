/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GoValidator.h"
#include "util/ExpressionUtils.h"
#include "common/base/Base.h"
#include "common/expression/VariableExpression.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/Logic.h"
#include "visitor/ExtractPropExprVisitor.h"

namespace nebula {
namespace graph {
Status GoValidator::validateImpl() {
    auto* goSentence = static_cast<GoSentence*>(sentence_);
    goCtx_ = getContext<GoContext>();
    goCtx_->inputVarName = inputVarName_;


    NG_RETURN_IF_ERROR(validateStep(goSentence->stepClause(), goCtx_->steps));
    NG_RETURN_IF_ERROR(validateStarts(goSentence->fromClause(), goCtx_->from));
    NG_RETURN_IF_ERROR(validateOver(goSentence->overClause(), goCtx_->over));
    NG_RETURN_IF_ERROR(validateWhere(goSentence->whereClause()));
    NG_RETURN_IF_ERROR(validateYield(goSentence->yieldClause()));
    NG_RETURN_IF_ERROR(validateTruncate(goSentence->truncateClause()));

    const auto& exprProps = goCtx_->exprProps;
    if (!exprProps.inputProps().empty() && goCtx_->from.fromType != kPipe) {
        return Status::SemanticError("$- must be referred in FROM before used in WHERE or YIELD");
    }

    if (!exprProps.varProps().empty() && goCtx_->from.fromType != kVariable) {
        return Status::SemanticError(
            "A variable must be referred in FROM before used in WHERE or YIELD");
    }

    if ((!exprProps.inputProps().empty() && !exprProps.varProps().empty()) ||
        exprProps.varProps().size() > 1) {
        return Status::SemanticError("Only support single input in a go sentence.");
    }

    NG_RETURN_IF_ERROR(buildColumns());
    return Status::OK();
}

Status GoValidator::validateWhere(WhereClause* where) {
    if (where == nullptr) {
        return Status::OK();
    }

    auto expr = where->filter();
    if (graph::ExpressionUtils::findAny(expr, {Expression::Kind::kAggregate})) {
        return Status::SemanticError("`%s', not support aggregate function in where sentence.",
                                     expr->toString().c_str());
    }
    where->setFilter(ExpressionUtils::rewriteLabelAttr2EdgeProp(expr));
    auto foldRes = ExpressionUtils::foldConstantExpr(where->filter());
    NG_RETURN_IF_ERROR(foldRes);

    auto filter = foldRes.value();
    auto typeStatus = deduceExprType(filter);
    NG_RETURN_IF_ERROR(typeStatus);
    auto type = typeStatus.value();
    if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
        type != Value::Type::__EMPTY__) {
        std::stringstream ss;
        ss << "`" << filter->toString() << "', expected Boolean, "
           << "but was `" << type << "'";
        return Status::SemanticError(ss.str());
    }

    NG_RETURN_IF_ERROR(deduceProps(filter, goCtx_->exprProps));
    goCtx_->filter = filter;
    return Status::OK();
}

Status GoValidator::validateTruncate(TruncateClause* truncate) {
    if (truncate == nullptr) {
        return Status::OK();
    }
    goCtx_->random = truncate->isSample();
    auto* tExpr = truncate->truncate();
    if (tExpr->kind() != Expression::Kind::kList) {
        return Status::SemanticError("`%s' type must be LIST", tExpr->toString().c_str());
    }
    const auto& steps = goCtx_->steps;
    // lenght of list must be equal to N step
    uint32_t totalSteps = steps.isMToN() ? steps.nSteps() : steps.steps();
    if (static_cast<const ListExpression*>(tExpr)->size() != totalSteps) {
        return Status::SemanticError(
            "`%s' length must be equal to %d", tExpr->toString().c_str(), totalSteps);
    }
    // check if value of list is non-integer
    auto existNonInteger = [](const Expression* expr) -> bool {
        if (expr->kind() != Expression::Kind::kConstant &&
            expr->kind() != Expression::Kind::kList) {
            return true;
        }
        if (expr->kind() == Expression::Kind::kConstant) {
            auto& val = static_cast<const ConstantExpression*>(expr)->value();
            if (!val.isInt()) {
                return true;
            }
        }
        return false;
    };
    FindVisitor visitor(existNonInteger);
    tExpr->accept(&visitor);
    auto res = visitor.results();
    if (res.size() == 1) {
        return Status::SemanticError("`%s' must be INT", res.front()->toString().c_str());
    }
    return Status::SemanticError("not implement it");
}

Status GoValidator::validateYield(YieldClause* yield) {
    if (yield == nullptr) {
        return Status::SemanticError("Yield clause nullptr.");
    }
    goCtx_->distinct = yield->isDistinct();
    const auto& over = goCtx_->over;
    auto* pool = qctx_->objPool();

    auto cols = yield->columns();
    if (cols.empty() && over.isOverAll) {
        DCHECK(!over.allEdges.empty());
        auto* newCols = qctx_->objPool()->add(new YieldColumns());
        for (const auto& e : over.allEdges) {
            auto* col = new YieldColumn(EdgeDstIdExpression::make(pool, e));
            newCols->addColumn(col);
            outputs_.emplace_back(col->name(), vidType_);
            NG_RETURN_IF_ERROR(deduceProps(col->expr(), goCtx_->exprProps));
        }
        goCtx_->yieldExpr = newCols;
        goCtx_->colNames = getOutColNames();
        return Status::OK();
    }

    for (auto col : cols) {
        col->setExpr(ExpressionUtils::rewriteLabelAttr2EdgeProp(col->expr()));
        NG_RETURN_IF_ERROR(invalidLabelIdentifiers(col->expr()));

        auto* colExpr = col->expr();
        if (graph::ExpressionUtils::findAny(colExpr, {Expression::Kind::kAggregate})) {
            return Status::SemanticError("`%s', not support aggregate function in go sentence.",
                                         col->toString().c_str());
        }
        // check input var expression
        auto typeStatus = deduceExprType(colExpr);
        NG_RETURN_IF_ERROR(typeStatus);
        auto type = typeStatus.value();
        outputs_.emplace_back(col->name(), type);

        NG_RETURN_IF_ERROR(deduceProps(colExpr, goCtx_->exprProps));
    }

    for (const auto& e : goCtx_->exprProps.edgeProps()) {
        auto found = std::find(over.edgeTypes.begin(), over.edgeTypes.end(), e.first);
        if (found == over.edgeTypes.end()) {
            return Status::SemanticError("Edges should be declared first in over clause.");
        }
    }
    goCtx_->yieldExpr = yield->yields();
    goCtx_->colNames = getOutColNames();
    return Status::OK();
}

void GoValidator::extractPropExprs(const Expression* expr) {
    ExtractPropExprVisitor visitor(
        vctx_, goCtx_->srcEdgePropsExpr, goCtx_->dstPropsExpr, inputPropCols_, propExprColMap_);
    const_cast<Expression*>(expr)->accept(&visitor);
}

Expression* GoValidator::rewrite2VarProp(const Expression* expr) {
    auto matcher = [this](const Expression* e) -> bool {
        return propExprColMap_.find(e->toString()) != propExprColMap_.end();
    };
    auto rewriter = [this](const Expression* e) -> Expression* {
        auto iter = propExprColMap_.find(e->toString());
        DCHECK(iter != propExprColMap_.end());
        return VariablePropertyExpression::make(qctx_->objPool(), "", iter->second->alias());
    };

    return RewriteVisitor::transform(expr, matcher, rewriter);
}

Status GoValidator::buildColumns() {
    const auto& exprProps = goCtx_->exprProps;
    const auto& dstTagProps = exprProps.dstTagProps();
    const auto& inputProps = exprProps.inputProps();
    const auto& varProps = exprProps.varProps();
    const auto& from = goCtx_->from;

    if (dstTagProps.empty() && inputProps.empty() && varProps.empty() &&
        from.fromType == FromType::kInstantExpr) {
        return Status::OK();
    }

    auto pool = qctx_->objPool();
    if (!exprProps.isAllPropsEmpty() || from.fromType != FromType::kInstantExpr) {
        goCtx_->srcEdgePropsExpr = pool->add(new YieldColumns());
    }

    if (!dstTagProps.empty()) {
        goCtx_->dstPropsExpr = pool->add(new YieldColumns());
    }

    if (!inputProps.empty() || !varProps.empty()) {
        inputPropCols_ = pool->add(new YieldColumns());
    }

    auto filter = goCtx_->filter;
    if (filter != nullptr) {
        extractPropExprs(filter);
        auto newFilter = filter->clone();
        goCtx_->filter = rewrite2VarProp(newFilter);
    }

    auto* newYieldExpr = pool->add(new YieldColumns());
    for (auto* yield : goCtx_->yieldExpr->columns()) {
        extractPropExprs(yield->expr());
        newYieldExpr->addColumn(new YieldColumn(rewrite2VarProp(yield->expr()), yield->alias()));
    }
    goCtx_->yieldExpr = newYieldExpr;

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
