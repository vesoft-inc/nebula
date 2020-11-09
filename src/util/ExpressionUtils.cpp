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

std::vector<const Expression*> ExpressionUtils::pullAnds(const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    auto *root = static_cast<const LogicalExpression*>(expr);
    std::vector<const Expression*> operands;

    // TODO(dutor) Deal with n-ary operands
    if (root->operand(0)->kind() != Expression::Kind::kLogicalAnd) {
        operands.emplace_back(root->operand(0));
    } else {
        auto ands = pullAnds(root->operand(0));
        operands.insert(operands.end(), ands.begin(), ands.end());
    }

    if (root->operand(1)->kind() != Expression::Kind::kLogicalAnd) {
        operands.emplace_back(root->operand(1));
    } else {
        auto ands = pullAnds(root->operand(1));
        operands.insert(operands.end(), ands.begin(), ands.end());
    }

    return operands;
}

std::vector<const Expression*> ExpressionUtils::pullOrs(const Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    auto *root = static_cast<const LogicalExpression*>(expr);
    std::vector<const Expression*> operands;

    // TODO(dutor) Deal with n-ary operands
    if (root->operand(0)->kind() != Expression::Kind::kLogicalOr) {
        operands.emplace_back(root->operand(0));
    } else {
        auto ands = pullOrs(root->operand(0));
        operands.insert(operands.end(), ands.begin(), ands.end());
    }

    if (root->operand(1)->kind() != Expression::Kind::kLogicalOr) {
        operands.emplace_back(root->operand(1));
    } else {
        auto ands = pullOrs(root->operand(1));
        operands.insert(operands.end(), ands.begin(), ands.end());
    }

    return operands;
}

}   // namespace graph
}   // namespace nebula
