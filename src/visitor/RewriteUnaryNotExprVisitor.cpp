/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteUnaryNotExprVisitor.h"

#include "context/QueryExpressionContext.h"
#include "util/ExpressionUtils.h"
namespace nebula {
namespace graph {

RewriteUnaryNotExprVisitor::RewriteUnaryNotExprVisitor(ObjectPool *objPool) : pool_(objPool) {}

void RewriteUnaryNotExprVisitor::visit(ConstantExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(UnaryExpression *expr) {
    auto operand = expr->operand();
    if (isUnaryNotExpr(expr)) {
        if (isUnaryNotExpr(operand)) {
            static_cast<UnaryExpression *>(operand)->operand()->accept(this);
            return;
        } else if (operand->isRelExpr() && operand->kind() != Expression::Kind::kRelREG) {
            auto reversedExpt =
                ExpressionUtils::reverseRelExpr(static_cast<RelationalExpression *>(operand));
            expr_ = pool_->add(reversedExpt.release());
            return;
        } else if (operand->isLogicalExpr()) {
            auto reducedExpr =
                ExpressionUtils::reverseLogicalExpr(static_cast<LogicalExpression *>(operand));
            auto reducedLogicalExpr = pool_->add(reducedExpr.release());
            reducedLogicalExpr->accept(this);
            return;
        }
    }
    operand->accept(this);

    if (operand != expr_) {
        expr->setOperand(expr_->clone().release());
    }
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(TypeCastingExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(LabelExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(LabelAttributeExpression *expr) {
    expr_ = expr;
}

// binary expression
void RewriteUnaryNotExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(AttributeExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteUnaryNotExprVisitor::visit(LogicalExpression *expr) {
    auto &operands = expr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        operands[i]->accept(this);
        if (expr_ != operands[i].get()) {
            expr->setOperand(i, expr_->clone().release());
        }
    }
    expr_ = expr;
}

// Rewrite Unary expresssion to Binary expression
void RewriteUnaryNotExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (expr_ != expr->left()) {
        expr->setLeft(expr_->clone().release());
    }
    expr->right()->accept(this);
    if (expr_ != expr->right()) {
        expr->setRight(expr_->clone().release());
    }
    expr_ = expr;
}

// function call
void RewriteUnaryNotExprVisitor::visit(FunctionCallExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(AggregateExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(UUIDExpression *expr) {
    expr_ = expr;
}

// variable expression
void RewriteUnaryNotExprVisitor::visit(VariableExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(VersionedVariableExpression *expr) {
    expr_ = expr;
}

// container expression
void RewriteUnaryNotExprVisitor::visit(ListExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (expr_ != item) {
            expr->setItem(i, expr_->clone());
        }
    }
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(SetExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (expr_ != item) {
            expr->setItem(i, expr_->clone());
        }
    }
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    for (auto i = 0u; i < items.size(); ++i) {
        auto &pair = items[i];
        auto item = const_cast<Expression *>(pair.second.get());
        item->accept(this);
        if (expr_ != item) {
            auto key = std::make_unique<std::string>(*pair.first);
            auto val = expr_->clone();
            expr->setItem(i, std::make_pair(std::move(key), std::move(val)));
        }
    }
    expr_ = expr;
}

// property Expression
void RewriteUnaryNotExprVisitor::visit(TagPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(InputPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(VariablePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(DestPropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(SourcePropertyExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeSrcIdExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeTypeExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeRankExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeDstIdExpression *expr) {
    expr_ = expr;
}

// vertex/edge expression
void RewriteUnaryNotExprVisitor::visit(VertexExpression *expr) {
    expr_ = expr;
}

void RewriteUnaryNotExprVisitor::visit(EdgeExpression *expr) {
    expr_ = expr;
}

// case expression
void RewriteUnaryNotExprVisitor::visit(CaseExpression *expr) {
    expr_ = expr;
}

// path build expression
void RewriteUnaryNotExprVisitor::visit(PathBuildExpression *expr) {
    expr_ = expr;
}

// column expression
void RewriteUnaryNotExprVisitor::visit(ColumnExpression *expr) {
    expr_ = expr;
}

// predicate expression
void RewriteUnaryNotExprVisitor::visit(PredicateExpression *expr) {
    expr_ = expr;
}

// list comprehension expression
void RewriteUnaryNotExprVisitor::visit(ListComprehensionExpression *expr) {
    expr_ = expr;
}

// reduce expression
void RewriteUnaryNotExprVisitor::visit(ReduceExpression *expr) {
    expr_ = expr;
}

bool RewriteUnaryNotExprVisitor::isUnaryNotExpr(const Expression *expr) {
    return expr->kind() == Expression::Kind::kUnaryNot;
}

}   // namespace graph
}   // namespace nebula
