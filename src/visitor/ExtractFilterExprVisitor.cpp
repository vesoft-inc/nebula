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

void ExtractFilterExprVisitor::visit(VariableExpression *) {
    canBePushed_ = false;
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

void ExtractFilterExprVisitor::visit(LogicalExpression *expr) {
    if (expr->kind() == Expression::Kind::kLogicalAnd) {
        expr->left()->accept(this);
        auto canBePushedLeft = canBePushed_;
        expr->right()->accept(this);
        auto canBePushedRight = canBePushed_;
        canBePushed_ = canBePushedLeft || canBePushedRight;
        if (canBePushed_) {
            if (!canBePushedLeft) {
                remainedExpr_ = expr->left()->clone();
                expr->setLeft(new ConstantExpression(true));
            } else if (!canBePushedRight) {
                remainedExpr_ = expr->right()->clone();
                expr->setRight(new ConstantExpression(true));
            }
        }
    } else {
        ExprVisitorImpl::visit(expr);
    }
}

}   // namespace graph
}   // namespace nebula
