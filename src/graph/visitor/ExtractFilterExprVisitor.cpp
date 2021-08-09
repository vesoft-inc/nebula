/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "ExtractFilterExprVisitor.h"

namespace nebula {
namespace graph {

void ExtractFilterExprVisitor::visit(ConstantExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(LabelExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(UUIDExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(VariableExpression *expr) {
    canBePushed_ = static_cast<VariableExpression *>(expr)->isInner();
}

void ExtractFilterExprVisitor::visit(VersionedVariableExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(TagPropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(EdgePropertyExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(InputPropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(VariablePropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(DestPropertyExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(SourcePropertyExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeSrcIdExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeTypeExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeRankExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(EdgeDstIdExpression *) {
    canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(VertexExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(EdgeExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(ColumnExpression *) {
    canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(LogicalExpression *expr) {
    if (expr->kind() != Expression::Kind::kLogicalAnd) {
        ExprVisitorImpl::visit(expr);
        return;
    }

    auto &operands = expr->operands();
    std::vector<bool> flags(operands.size(), false);
    auto canBePushed = false;
    for (auto i = 0u; i < operands.size(); i++) {
        canBePushed_ = true;
        operands[i]->accept(this);
        flags[i] = canBePushed_;
        canBePushed = canBePushed || canBePushed_;
    }
    canBePushed_ = canBePushed;
    if (!canBePushed_) {
        return;
    }
    std::vector<Expression*> remainedOperands;
    for (auto i = 0u; i < operands.size(); i++) {
        if (!flags[i]) {
            remainedOperands.emplace_back(operands[i]->clone());
            expr->setOperand(i, ConstantExpression::make(pool_, true));
        }
    }
    if (remainedOperands.empty()) {
        return;
    }
    if (remainedOperands.size() > 1) {
        auto remainedExpr = LogicalExpression::makeAnd(pool_);
        remainedExpr->setOperands(std::move(remainedOperands));
        remainedExpr_ = std::move(remainedExpr);
    } else {
        remainedExpr_ = std::move(remainedOperands[0]);
    }
}

void ExtractFilterExprVisitor::visit(SubscriptRangeExpression *) {
    canBePushed_ = false;
}

}   // namespace graph
}   // namespace nebula
