/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ExpressionUtils.h"

#include "visitor/FoldConstantExprVisitor.h"

namespace nebula {
namespace graph {

std::unique_ptr<Expression> ExpressionUtils::foldConstantExpr(const Expression *expr) {
    auto newExpr = expr->clone();
    FoldConstantExprVisitor visitor;
    newExpr->accept(&visitor);
    if (visitor.canBeFolded()) {
        return std::unique_ptr<Expression>(visitor.fold(newExpr.get()));
    }
    return newExpr;
}

}   // namespace graph
}   // namespace nebula
