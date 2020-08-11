/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"
#include "planner/Query.h"


namespace nebula {
namespace graph {

Status GroupByValidator::validateImpl() {
    auto *groupBySentence = static_cast<GroupBySentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateGroup(groupBySentence->groupClause()));
    NG_RETURN_IF_ERROR(validateYield(groupBySentence->yieldClause()));
    NG_RETURN_IF_ERROR(checkInputProps());
    NG_RETURN_IF_ERROR(checkVarProps());

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
        return Status::SemanticError("Only support input and variable in GroupBy sentence.");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable in GroupBy sentence.");
    }
    return Status::OK();
}

Status GroupByValidator::checkInputProps() const {
    auto& inputProps = const_cast<ExpressionProps*>(&exprProps_)->inputProps();
    if (inputs_.empty() && !inputProps.empty()) {
        return Status::SemanticError("no inputs for GroupBy.");
    }
    for (auto &prop : inputProps) {
        DCHECK_NE(prop, "*");
        NG_RETURN_IF_ERROR(checkPropNonexistOrDuplicate(inputs_, prop, "GroupBy sentence"));
    }
    return Status::OK();
}

Status GroupByValidator::checkVarProps() const {
    auto& varProps = const_cast<ExpressionProps*>(&exprProps_)->varProps();
    for (auto &pair : varProps) {
        auto &var = pair.first;
        if (!vctx_->existVar(var)) {
            return Status::SemanticError("variable `%s' not exist.", var.c_str());
        }
        auto &props = vctx_->getVar(var);
        for (auto &prop : pair.second) {
            DCHECK_NE(prop, "*");
            NG_RETURN_IF_ERROR(checkPropNonexistOrDuplicate(props, prop, "GroupBy sentence"));
        }
    }
    return Status::OK();
}

Status GroupByValidator::validateYield(const YieldClause *yieldClause) {
    std::vector<YieldColumn*> columns;
    if (yieldClause != nullptr) {
        columns = yieldClause->columns();
    }
    if (columns.empty()) {
        return Status::SemanticError("Yield cols is Empty");
    }

    for (auto* col : columns) {
        auto fun = col->getAggFunName();
        if (!fun.empty()) {
            auto iter = AggFun::nameIdMap_.find(fun);
            if (iter == AggFun::nameIdMap_.end()) {
                return Status::SemanticError("Unkown aggregate function `%s`", fun.c_str());
            }
            if (iter->second != AggFun::Function::kCount && col->expr()->toString() == "*") {
                return Status::SemanticError("`%s` invaild, * valid in count.",
                                             col->toString().c_str());
            }
        }

        // todo(jmq) count(distinct)
        groupItems_.emplace_back(Aggregate::GroupItem{col->expr(), AggFun::nameIdMap_[fun], false});

        auto status = deduceExprType(col->expr());
        NG_RETURN_IF_ERROR(status);
        auto type = std::move(status).value();
        auto name = deduceColName(col);
        outputs_.emplace_back(name, type);
        outputColumnNames_.emplace_back(std::move(name));
        // todo(jmq) extend $-.*

        yieldCols_.emplace_back(col);
        if (col->alias() != nullptr) {
            aliases_.emplace(*col->alias(), col);
        }

        // check input yield filed without agg function and not in group cols
        ExpressionProps yieldProps;
        NG_RETURN_IF_ERROR(deduceProps(col->expr(), yieldProps));
        if (col->getAggFunName().empty()) {
            if (!yieldProps.inputProps().empty()) {
                if (!exprProps_.isSubsetOfInput(yieldProps.inputProps())) {
                    return Status::SemanticError("Yield `%s` isn't in output fields",
                                                 col->toString().c_str());
                }
            } else if (!yieldProps.varProps().empty()) {
                if (!exprProps_.isSubsetOfVar(yieldProps.varProps())) {
                    return Status::SemanticError("Yield `%s` isn't in output fields",
                                                 col->toString().c_str());
                }
            }
        }
        exprProps_.unionProps(std::move(yieldProps));
    }
    return Status::OK();
}


Status GroupByValidator::validateGroup(const GroupClause *groupClause) {
    std::vector<YieldColumn*> columns;
    if (groupClause != nullptr) {
        columns = groupClause->columns();
    }

    if (columns.empty()) {
        return Status::SemanticError("Group cols is Empty");
    }
    for (auto* col : columns) {
        if (col->expr()->kind() != Expression::Kind::kInputProperty &&
            col->expr()->kind() != Expression::Kind::kFunctionCall) {
            return Status::SemanticError("Group `%s` invalid", col->expr()->toString().c_str());
        }
        if (!col->getAggFunName().empty()) {
            return Status::SemanticError("Use invalid group function `%s`",
                                         col->getAggFunName().c_str());
        }
        NG_RETURN_IF_ERROR(deduceExprType(col->expr()));
        NG_RETURN_IF_ERROR(deduceProps(col->expr(), exprProps_));

        groupCols_.emplace_back(col);
        groupKeys_.emplace_back(col->expr());
    }
    return Status::OK();
}

Status GroupByValidator::toPlan() {
    auto *plan = qctx_->plan();
    auto *groupBy =
        Aggregate::make(plan, nullptr, std::move(groupKeys_), std::move(groupItems_));
    groupBy->setColNames(std::vector<std::string>(outputColumnNames_));
    root_ = groupBy;
    tail_ = groupBy;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
