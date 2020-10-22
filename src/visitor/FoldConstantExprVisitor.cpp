/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/FoldConstantExprVisitor.h"

#include "context/QueryExpressionContext.h"

namespace nebula {
namespace graph {

void FoldConstantExprVisitor::visit(ConstantExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = true;
}

void FoldConstantExprVisitor::visit(UnaryExpression *expr) {
    expr->operand()->accept(this);
    if (canBeFolded_ && expr->operand()->kind() != Expression::Kind::kConstant) {
        expr->setOperand(fold(expr->operand()));
    }
}

void FoldConstantExprVisitor::visit(TypeCastingExpression *expr) {
    expr->operand()->accept(this);
    if (canBeFolded_ && expr->operand()->kind() != Expression::Kind::kConstant) {
        expr->setOperand(fold(expr->operand()));
    }
}

void FoldConstantExprVisitor::visit(LabelExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(LabelAttributeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// binary expression
void FoldConstantExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void FoldConstantExprVisitor::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void FoldConstantExprVisitor::visit(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void FoldConstantExprVisitor::visit(AttributeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(LogicalExpression *expr) {
    visitBinaryExpr(expr);
}

// function call
void FoldConstantExprVisitor::visit(FunctionCallExpression *expr) {
    bool canBeFolded = true;
    for (auto &arg : expr->args()->args()) {
        if (arg->kind() != Expression::Kind::kConstant) {
            arg->accept(this);
            if (canBeFolded_) {
                arg.reset(fold(arg.get()));
            } else {
                canBeFolded = false;
            }
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(UUIDExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// variable expression
void FoldConstantExprVisitor::visit(VariableExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(VersionedVariableExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// container expression
void FoldConstantExprVisitor::visit(ListExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (!canBeFolded_) {
            canBeFolded = false;
            continue;
        }
        if (item->kind() != Expression::Kind::kConstant) {
            expr->setItem(i, std::unique_ptr<Expression>{fold(item)});
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(SetExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto item = items[i].get();
        item->accept(this);
        if (!canBeFolded_) {
            canBeFolded = false;
            continue;
        }
        if (item->kind() != Expression::Kind::kConstant) {
            expr->setItem(i, std::unique_ptr<Expression>{fold(item)});
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto &pair = items[i];
        auto item = const_cast<Expression *>(pair.second.get());
        if (!canBeFolded_) {
            canBeFolded = false;
            continue;
        }
        if (item->kind() != Expression::Kind::kConstant) {
            auto key = std::make_unique<std::string>(*pair.first);
            auto val = std::unique_ptr<Expression>(fold(item));
            expr->setItem(i, std::make_pair(std::move(key), std::move(val)));
        }
    }
    canBeFolded_ = canBeFolded;
}

// property Expression
void FoldConstantExprVisitor::visit(TagPropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgePropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(InputPropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(VariablePropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(DestPropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(SourcePropertyExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeSrcIdExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeTypeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeRankExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeDstIdExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

// vertex/edge expression
void FoldConstantExprVisitor::visit(VertexExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visit(EdgeExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    auto leftCanBeFolded = canBeFolded_;
    if (leftCanBeFolded && expr->left()->kind() != Expression::Kind::kConstant) {
        expr->setLeft(fold(expr->left()));
    }
    expr->right()->accept(this);
    auto rightCanBeFolded = canBeFolded_;
    if (rightCanBeFolded && expr->right()->kind() != Expression::Kind::kConstant) {
        expr->setRight(fold(expr->right()));
    }
    canBeFolded_ = leftCanBeFolded && rightCanBeFolded;
}

Expression *FoldConstantExprVisitor::fold(Expression *expr) const {
    QueryExpressionContext ctx;
    auto value = expr->eval(ctx(nullptr));
    return new ConstantExpression(std::move(value));
}

}   // namespace graph
}   // namespace nebula
