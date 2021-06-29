/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/OrderByValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/Query.h"
#include "common/expression/LabelExpression.h"

namespace nebula {
namespace graph {
Status OrderByValidator::validateImpl() {
    auto sentence = static_cast<OrderBySentence*>(sentence_);
    outputs_ = inputCols();
    auto &factors = sentence->factors();
    auto *pool = qctx_->objPool();
    for (auto &factor : factors) {
        if (factor->expr()->kind() == Expression::Kind::kLabel) {
            auto *label = static_cast<const LabelExpression*>(factor->expr());
            auto *expr = InputPropertyExpression::make(pool, label->name());
            factor->setExpr(expr);
        }
        if (factor->expr()->kind() != Expression::Kind::kInputProperty) {
            return Status::SemanticError("Order by with invalid expression `%s'",
                                          factor->expr()->toString().c_str());
        }
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        NG_RETURN_IF_ERROR(deduceExprType(expr));
        auto& name = expr->prop();
        auto eq = [&](const ColDef& col) { return col.name == name; };
        auto iter = std::find_if(outputs_.cbegin(), outputs_.cend(), eq);
        size_t colIdx = std::distance(outputs_.cbegin(), iter);
        colOrderTypes_.emplace_back(std::make_pair(colIdx, factor->orderType()));
    }

    return Status::OK();
}

Status OrderByValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto *sortNode = Sort::make(qctx_, plan->root(), std::move(colOrderTypes_));
    std::vector<std::string> colNames;
    for (auto &col : outputs_) {
        colNames.emplace_back(col.name);
    }
    sortNode->setColNames(std::move(colNames));
    root_ = sortNode;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
