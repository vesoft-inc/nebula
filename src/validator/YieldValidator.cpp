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
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "visitor/FoldConstantExprVisitor.h"

namespace nebula {
namespace graph {

YieldValidator::YieldValidator(Sentence *sentence, QueryContext *qctx)
    : Validator(sentence, qctx) {
        setNoSpaceRequired();
    }

Status YieldValidator::validateImpl() {
    if (qctx_->vctx()->spaceChosen()) {
        space_ = vctx_->whichSpace();
    }

    auto yield = static_cast<YieldSentence *>(sentence_);
    if (yield->hasAgg()) {
        NG_RETURN_IF_ERROR(makeImplicitGroupByValidator());
    }
    NG_RETURN_IF_ERROR(validateWhere(yield->where()));
    if (groupByValidator_) {
        NG_RETURN_IF_ERROR(validateImplicitGroupBy());
    } else {
        NG_RETURN_IF_ERROR(validateYieldAndBuildOutputs(yield->yield()));
    }

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

    if (!groupByValidator_ && exprProps_.inputProps().empty()
        && exprProps_.varProps().empty() && inputVarName_.empty()) {
        // generate constant expression result into querycontext
        genConstantExprValues();
    }

    if (!exprProps_.varProps().empty() && !userDefinedVarNameList_.empty()) {
        // TODO: Support Multiple userDefinedVars
        if (userDefinedVarNameList_.size() != 1) {
            return Status::SemanticError("Multiple user defined vars not supported yet.");
        }
        userDefinedVarName_ = *userDefinedVarNameList_.begin();
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

Status YieldValidator::makeImplicitGroupByValidator() {
    auto* groupSentence = qctx()->objPool()->add(
            new GroupBySentence(
                static_cast<YieldSentence *>(sentence_)->yield()->clone().release(),
                nullptr, nullptr));
    groupByValidator_ = std::make_unique<GroupByValidator>(groupSentence, qctx());
    groupByValidator_->setInputCols(inputs_);

    return Status::OK();
}

Status YieldValidator::validateImplicitGroupBy() {
    NG_RETURN_IF_ERROR(groupByValidator_->validateImpl());
    inputs_ = groupByValidator_->inputCols();
    outputs_ = groupByValidator_->outputCols();
    exprProps_.unionProps(groupByValidator_->exprProps());

    const auto& groupVars = groupByValidator_->userDefinedVarNameList();
    // TODO: Support Multiple userDefinedVars
    userDefinedVarNameList_.insert(groupVars.begin(), groupVars.end());

    return Status::OK();
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
                continue;
            }
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

    std::string inputVar;
    if (!userDefinedVarName_.empty()) {
        inputVar = userDefinedVarName_;
    } else if (!constantExprVar_.empty()) {
        inputVar = constantExprVar_;
    }

    Filter *filter = nullptr;
    if (yield->where()) {
        filter = Filter::make(qctx_, nullptr, filterCondition_);
        std::vector<std::string> colNames(inputs_.size());
        std::transform(
            inputs_.cbegin(), inputs_.cend(), colNames.begin(), [](auto &col) { return col.name; });
        filter->setColNames(std::move(colNames));
    }

    SingleInputNode *dedupDep = nullptr;

    if (groupByValidator_) {
        groupByValidator_->toPlan();
        auto* groupByValidatorRoot = groupByValidator_->root();
        auto* groupByValidatorTail = groupByValidator_->tail();
        // groupBy validator only gen Project or Aggregate Node
        DCHECK(groupByValidatorRoot->isSingleInput());
        DCHECK(groupByValidatorTail->isSingleInput());
        dedupDep = static_cast<SingleInputNode*>(groupByValidatorRoot);
        if (filter != nullptr) {
            if (!inputVar.empty()) {
                filter->setInputVar(inputVar);
            }
            static_cast<SingleInputNode*>(groupByValidatorTail)->dependsOn(filter);
            static_cast<SingleInputNode*>(groupByValidatorTail)->setInputVar(filter->outputVar());
            tail_ = filter;
        } else {
            tail_ = groupByValidatorTail;
            if (!inputVar.empty()) {
                static_cast<SingleInputNode*>(tail_)->setInputVar(inputVar);
            }
        }
    } else {
        dedupDep = Project::make(qctx_, filter, columns_);
        dedupDep->setColNames(std::move(outputColumnNames_));
        if (filter != nullptr) {
            dedupDep->setInputVar(filter->outputVar());
            tail_ = filter;
        } else {
            tail_ = dedupDep;
        }
        // Otherwise the input of tail_ would be set by pipe.
        if (!inputVar.empty()) {
            static_cast<SingleInputNode *>(tail_)->setInputVar(inputVar);
        }
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
