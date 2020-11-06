/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/YieldValidator.h"

#include "common/expression/Expression.h"
#include "context/QueryContext.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"
#include "util/ExpressionUtils.h"
#include "visitor/FoldConstantExprVisitor.h"

namespace nebula {
namespace graph {

YieldValidator::YieldValidator(Sentence *sentence, QueryContext *qctx)
    : Validator(sentence, qctx) {}

Status YieldValidator::validateImpl() {
    auto yield = static_cast<YieldSentence *>(sentence_);
    NG_RETURN_IF_ERROR(validateYieldAndBuildOutputs(yield->yield()));
    NG_RETURN_IF_ERROR(validateWhere(yield->where()));

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty() ||
        !exprProps_.edgeProps().empty()) {
        return Status::SemanticError("Only support input and variable in yield sentence.");
    }

    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable.");
    }

    if (!exprProps_.varProps().empty() && exprProps_.varProps().size() > 1) {
        return Status::SemanticError("Only one variable allowed to use.");
    }

    if (hasAggFun_) {
        NG_RETURN_IF_ERROR(checkAggFunAndBuildGroupItems(yield->yield()));
    }

    if (exprProps_.inputProps().empty() && exprProps_.varProps().empty()) {
        // generate constant expression result into querycontext
        genConstantExprValues();
    }

    return Status::OK();
}

Status YieldValidator::checkAggFunAndBuildGroupItems(const YieldClause *clause) {
    auto yield = clause->yields();
    for (auto column : yield->columns()) {
        auto expr = column->expr();
        auto fun = column->getAggFunName();
        if (!evaluableExpr(expr) && fun.empty()) {
            return Status::SemanticError(
                "Input columns without aggregation are not supported in YIELD statement "
                "without GROUP BY, near `%s'",
                expr->toString().c_str());
        }

        groupItems_.emplace_back(Aggregate::GroupItem{expr, AggFun::nameIdMap_[fun], false});
    }
    return Status::OK();
}

Status YieldValidator::makeOutputColumn(YieldColumn *column) {
    columns_->addColumn(column);

    auto expr = column->expr();
    DCHECK(expr != nullptr);

    NG_RETURN_IF_ERROR(deduceProps(expr, exprProps_));

    auto status = deduceExprType(expr);
    NG_RETURN_IF_ERROR(status);
    auto type = std::move(status).value();

    auto name = deduceColName(column);
    outputColumnNames_.emplace_back(name);

    // Constant expression folding must be after type deduction
    FoldConstantExprVisitor visitor;
    expr->accept(&visitor);
    if (visitor.canBeFolded()) {
        column->setExpr(visitor.fold(expr));
    }

    outputs_.emplace_back(name, type);
    return Status::OK();
}

void YieldValidator::genConstantExprValues() {
    constantExprVar_ = vctx_->anonVarGen()->getVar();
    DataSet ds;
    ds.colNames = outputColumnNames_;
    QueryExpressionContext ctx;
    Row row;
    for (auto &column : columns_->columns()) {
        row.values.emplace_back(Expression::eval(column->expr(), ctx(nullptr)));
    }
    ds.emplace_back(std::move(row));
    qctx_->ectx()->setResult(constantExprVar_,
                             ResultBuilder().value(Value(std::move(ds))).finish());
}

Status YieldValidator::validateYieldAndBuildOutputs(const YieldClause *clause) {
    auto columns = clause->columns();
    columns_ = qctx_->objPool()->add(new YieldColumns);
    for (auto column : columns) {
        auto expr = DCHECK_NOTNULL(column->expr());
        NG_RETURN_IF_ERROR(invalidLabelIdentifiers(expr));

        if (expr->kind() == Expression::Kind::kInputProperty) {
            auto ipe = static_cast<const InputPropertyExpression *>(expr);
            // Get all props of input expression could NOT be a part of another expression. So
            // it's always a root of expression.
            if (*ipe->prop() == "*") {
                for (auto &colDef : inputs_) {
                    auto newExpr = new InputPropertyExpression(new std::string(colDef.name));
                    NG_RETURN_IF_ERROR(makeOutputColumn(new YieldColumn(newExpr)));
                }
                if (!column->getAggFunName().empty()) {
                    return Status::SemanticError("could not apply aggregation function on `$-.*'");
                }
                continue;
            }
        } else if (expr->kind() == Expression::Kind::kVarProperty) {
            auto vpe = static_cast<const VariablePropertyExpression *>(expr);
            // Get all props of variable expression is same as above input property expression.
            if (*vpe->prop() == "*") {
                auto var = DCHECK_NOTNULL(vpe->sym());
                if (!vctx_->existVar(*var)) {
                    return Status::SemanticError("variable `%s' not exists.", var->c_str());
                }
                auto &varColDefs = vctx_->getVar(*var);
                for (auto &colDef : varColDefs) {
                    auto newExpr = new VariablePropertyExpression(new std::string(*var),
                                                                  new std::string(colDef.name));
                    NG_RETURN_IF_ERROR(makeOutputColumn(new YieldColumn(newExpr)));
                }
                if (!column->getAggFunName().empty()) {
                    return Status::SemanticError("could not apply aggregation function on `$%s.*'",
                                                 var->c_str());
                }
                continue;
            }
        }

        auto fun = column->getAggFunName();
        if (!fun.empty()) {
            auto foundAgg = AggFun::nameIdMap_.find(fun);
            if (foundAgg == AggFun::nameIdMap_.end()) {
                return Status::SemanticError("Unkown aggregate function: `%s'", fun.c_str());
            }
            hasAggFun_ = true;
        }

        NG_RETURN_IF_ERROR(makeOutputColumn(column->clone().release()));
    }

    return Status::OK();
}

Status YieldValidator::validateWhere(const WhereClause *clause) {
    Expression *filter = nullptr;
    if (clause != nullptr) {
        filter = clause->filter();
    }
    if (filter != nullptr) {
        NG_RETURN_IF_ERROR(deduceProps(filter, exprProps_));
        auto newFilter = ExpressionUtils::foldConstantExpr(filter);
        filterCondition_ = qctx_->objPool()->add(newFilter.release());
    }
    return Status::OK();
}

Status YieldValidator::toPlan() {
    auto yield = static_cast<const YieldSentence *>(sentence_);

    Filter *filter = nullptr;
    if (yield->where()) {
        filter = Filter::make(qctx_, nullptr, filterCondition_);
        std::vector<std::string> colNames(inputs_.size());
        std::transform(
            inputs_.cbegin(), inputs_.cend(), colNames.begin(), [](auto &col) { return col.name; });
        filter->setColNames(std::move(colNames));
        if (!constantExprVar_.empty()) {
            filter->setInputVar(constantExprVar_);
        }
    }

    SingleInputNode *dedupDep = nullptr;
    if (!hasAggFun_) {
        dedupDep = Project::make(qctx_, filter, columns_);
    } else {
        // We do not use group items later, so move it is safe
        dedupDep = Aggregate::make(qctx_, filter, {}, std::move(groupItems_));
    }

    dedupDep->setColNames(std::move(outputColumnNames_));
    if (filter != nullptr) {
        dedupDep->setInputVar(filter->outputVar());
        tail_ = filter;
    } else {
        tail_ = dedupDep;
    }

    if (!exprProps_.varProps().empty()) {
        DCHECK_EQ(exprProps_.varProps().size(), 1u);
        auto var = exprProps_.varProps().cbegin()->first;
        static_cast<SingleInputNode *>(tail_)->setInputVar(var);
    }

    if (yield->yield()->isDistinct()) {
        auto dedup = Dedup::make(qctx_, dedupDep);
        dedup->setColNames(dedupDep->colNames());
        dedup->setInputVar(dedupDep->outputVar());
        root_ = dedup;
    } else {
        root_ = dedupDep;
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
