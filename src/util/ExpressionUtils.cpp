/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ExpressionUtils.h"

#include <memory>

#include "common/expression/PropertyExpression.h"
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

Expression* ExpressionUtils::pullAnds(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
    auto *logic = static_cast<LogicalExpression*>(expr);
    std::vector<std::unique_ptr<Expression>> operands;
    pullAndsImpl(logic, operands);
    logic->setOperands(std::move(operands));
    return logic;
}

Expression* ExpressionUtils::pullOrs(Expression *expr) {
    DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
    auto *logic = static_cast<LogicalExpression*>(expr);
    std::vector<std::unique_ptr<Expression>> operands;
    pullOrsImpl(logic, operands);
    logic->setOperands(std::move(operands));
    return logic;
}

void
ExpressionUtils::pullAndsImpl(LogicalExpression *expr,
                              std::vector<std::unique_ptr<Expression>> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalAnd) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullAndsImpl(static_cast<LogicalExpression*>(operand.get()), operands);
    }
}

void
ExpressionUtils::pullOrsImpl(LogicalExpression *expr,
                             std::vector<std::unique_ptr<Expression>> &operands) {
    for (auto &operand : expr->operands()) {
        if (operand->kind() != Expression::Kind::kLogicalOr) {
            operands.emplace_back(std::move(operand));
            continue;
        }
        pullOrsImpl(static_cast<LogicalExpression*>(operand.get()), operands);
    }
}

VariablePropertyExpression *ExpressionUtils::newVarPropExpr(const std::string &prop,
                                                            const std::string &var) {
    return new VariablePropertyExpression(new std::string(var), new std::string(prop));
}

std::unique_ptr<InputPropertyExpression> ExpressionUtils::inputPropExpr(const std::string &prop) {
    return std::make_unique<InputPropertyExpression>(new std::string(prop));
}

}   // namespace graph
}   // namespace nebula
