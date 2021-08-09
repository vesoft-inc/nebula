/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

void ExprVisitorImpl::visit(UnaryExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->operand()->accept(this);
}

void ExprVisitorImpl::visit(TypeCastingExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->operand()->accept(this);
}

// binary expression
void ExprVisitorImpl::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(SubscriptExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(AttributeExpression *expr) {
    visitBinaryExpr(expr);
}

void ExprVisitorImpl::visit(LogicalExpression *expr) {
    for (auto &operand : expr->operands()) {
        operand->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(LabelAttributeExpression *expr) {
    DCHECK(ok()) << expr->toString();
    const_cast<LabelExpression *>(expr->left())->accept(this);
    if (ok()) {
        const_cast<ConstantExpression *>(expr->right())->accept(this);
    }
}

// function call
void ExprVisitorImpl::visit(FunctionCallExpression *expr) {
    DCHECK(ok()) << expr->toString();
    for (const auto &arg : expr->args()->args()) {
        arg->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(AggregateExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->arg()->accept(this);
}

// container expression
void ExprVisitorImpl::visit(ListExpression *expr) {
    DCHECK(ok()) << expr->toString();
    for (auto &item : expr->items()) {
        item->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(SetExpression *expr) {
    DCHECK(ok()) << expr->toString();
    for (auto &item : expr->items()) {
        item->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(MapExpression *expr) {
    DCHECK(ok()) << expr->toString();
    for (auto &pair : expr->items()) {
        pair.second->accept(this);
        if (!ok()) {
            break;
        }
    }
}

// case expression
void ExprVisitorImpl::visit(CaseExpression *expr) {
    DCHECK(ok()) << expr->toString();
    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        if (!ok()) {
            return;
        }
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        if (!ok()) {
            return;
        }
    }
    for (const auto &whenThen : expr->cases()) {
        whenThen.when->accept(this);
        if (!ok()) {
            break;
        }
        whenThen.then->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visitBinaryExpr(BinaryExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->left()->accept(this);
    if (ok()) {
        expr->right()->accept(this);
    }
}

void ExprVisitorImpl::visit(PathBuildExpression *expr) {
    DCHECK(ok()) << expr->toString();
    for (auto &item : expr->items()) {
        item->accept(this);
        if (!ok()) {
            break;
        }
    }
}

void ExprVisitorImpl::visit(PredicateExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->collection()->accept(this);
    if (!ok()) return;
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (!ok()) {
            return;
        }
    }
}

void ExprVisitorImpl::visit(ListComprehensionExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->collection()->accept(this);
    if (!ok()) return;
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (!ok()) return;
    }
    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
        if (!ok()) return;
    }
}

void ExprVisitorImpl::visit(ReduceExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->initial()->accept(this);
    if (!ok()) return;
    expr->collection()->accept(this);
    if (!ok()) return;
    expr->mapping()->accept(this);
    if (!ok()) return;
}

void ExprVisitorImpl::visit(SubscriptRangeExpression *expr) {
    DCHECK(ok()) << expr->toString();
    expr->list()->accept(this);
    if (!ok()) {
        return;
    }
    if (expr->lo() != nullptr) {
        expr->lo()->accept(this);
        if (!ok()) {
            return;
        }
    }
    if (expr->hi() != nullptr) {
        expr->hi()->accept(this);
        if (!ok()) {
            return;
        }
    }
}

}   // namespace graph
}   // namespace nebula
