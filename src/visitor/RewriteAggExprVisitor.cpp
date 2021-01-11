/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteAggExprVisitor.h"
#include "util/ExpressionUtils.h"

#include "common/base/Logging.h"

namespace nebula {
namespace graph {

RewriteAggExprVisitor::RewriteAggExprVisitor(std::string* var,
                                             std::string* prop) {
    var_.reset(var);
    prop_.reset(prop);
}

void RewriteAggExprVisitor::visit(TypeCastingExpression* expr) {
    if (isAggExpr(expr->operand())) {
        expr->setOperand(ExpressionUtils::newVarPropExpr(prop_->c_str(), var_->c_str()));
    } else {
        expr->operand()->accept(this);
    }
}

// rewrite AggregateExpression to VariablePropertyExpression
void RewriteAggExprVisitor::visit(FunctionCallExpression* expr) {
    for (auto& arg : expr->args()->args()) {
        if (isAggExpr(arg.get())) {
            arg.reset(new VariablePropertyExpression(var_.release(),
                                                     prop_.release()));
        } else {
            arg->accept(this);
        }
    }
}

void RewriteAggExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(static_cast<BinaryExpression*>(expr));
}

void RewriteAggExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    DCHECK(ok());
    auto* lhs = expr->left();
    if (isAggExpr(lhs)) {
        expr->setLeft(new VariablePropertyExpression(std::move(var_).release(),
                                                     std::move(prop_).release()));
        // only support rewrite single agg expr
        return;
    } else {
        lhs->accept(this);
    }
    auto* rhs = expr->right();
    if (isAggExpr(rhs)) {
        expr->setRight(new VariablePropertyExpression(std::move(var_).release(),
                                                     std::move(prop_).release()));
        return;
    } else {
        rhs->accept(this);
    }
}
bool RewriteAggExprVisitor::isAggExpr(const Expression* expr) {
    return expr->kind() == Expression::Kind::kAggregate;
}


}   // namespace graph
}   // namespace nebula
