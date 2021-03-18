/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/CollectAllExprsVisitor.h"

namespace nebula {
namespace graph {

CollectAllExprsVisitor::CollectAllExprsVisitor(
    const std::unordered_set<Expression::Kind> &exprKinds)
    : exprKinds_(exprKinds) {}

void CollectAllExprsVisitor::visit(TypeCastingExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectAllExprsVisitor::visit(UnaryExpression *expr) {
    collectExpr(expr);
    expr->operand()->accept(this);
}

void CollectAllExprsVisitor::visit(FunctionCallExpression *expr) {
    collectExpr(expr);
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
    }
}

void CollectAllExprsVisitor::visit(AggregateExpression *expr) {
    collectExpr(expr);
    expr->arg()->accept(this);
}

void CollectAllExprsVisitor::visit(ListExpression *expr) {
    collectExpr(expr);
    for (auto &item : expr->items()) {
        item->accept(this);
    }
}

void CollectAllExprsVisitor::visit(SetExpression *expr) {
    collectExpr(expr);
    for (auto &item : expr->items()) {
        item->accept(this);
    }
}

void CollectAllExprsVisitor::visit(MapExpression *expr) {
    collectExpr(expr);
    for (const auto &pair : expr->items()) {
        pair.second->accept(this);
    }
}

void CollectAllExprsVisitor::visit(ConstantExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(EdgePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(TagPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(InputPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(VariablePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(SourcePropertyExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(DestPropertyExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(EdgeSrcIdExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(EdgeTypeExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(EdgeRankExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(EdgeDstIdExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(UUIDExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(VariableExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(VersionedVariableExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(LabelExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(LabelAttributeExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(VertexExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(EdgeExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(ColumnExpression *expr) {
    collectExpr(expr);
}

void CollectAllExprsVisitor::visit(CaseExpression *expr) {
    collectExpr(expr);
    if (expr->hasCondition()) {
        expr->condition()->accept(this);
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
    }
    for (const auto &whenThen : expr->cases()) {
        whenThen.when->accept(this);
        whenThen.then->accept(this);
    }
}

void CollectAllExprsVisitor::visit(ListComprehensionExpression* expr) {
    collectExpr(expr);
    expr->collection()->accept(this);
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
    }
    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
    }
}

void CollectAllExprsVisitor::visit(PredicateExpression *expr) {
    collectExpr(expr);
    expr->collection()->accept(this);
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
    }
}

void CollectAllExprsVisitor::visit(ReduceExpression *expr) {
    collectExpr(expr);
    expr->initial()->accept(this);
    expr->collection()->accept(this);
    expr->mapping()->accept(this);
}

void CollectAllExprsVisitor::visitBinaryExpr(BinaryExpression *expr) {
    collectExpr(expr);
    expr->left()->accept(this);
    expr->right()->accept(this);
}

void CollectAllExprsVisitor::collectExpr(const Expression *expr) {
    if (exprKinds_.find(expr->kind()) != exprKinds_.cend()) {
        exprs_.push_back(expr);
    }
}

}   // namespace graph
}   // namespace nebula
