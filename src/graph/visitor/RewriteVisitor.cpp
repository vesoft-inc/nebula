/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {

void RewriteVisitor::visit(TypeCastingExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    if (matcher_(expr->operand())) {
        expr->setOperand(rewriter_(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}

void RewriteVisitor::visit(UnaryExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    if (matcher_(expr->operand())) {
        expr->setOperand(rewriter_(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}

void RewriteVisitor::visit(FunctionCallExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    for (auto &arg : expr->args()->args()) {
        if (matcher_(arg)) {
            arg = rewriter_(arg);
        } else {
            arg->accept(this);
        }
    }
}

void RewriteVisitor::visit(AggregateExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    auto arg = expr->arg();
    if (matcher_(arg)) {
        expr->setArg(rewriter_(arg));
    } else {
        arg->accept(this);
    }
}

void RewriteVisitor::visit(LogicalExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    for (auto &operand : expr->operands()) {
        if (matcher_(operand)) {
            const_cast<Expression* &>(operand) = rewriter_(operand);
        } else {
            operand->accept(this);
        }
    }
}

void RewriteVisitor::visit(ListExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    auto &items = expr->items();
    for (auto &item : items) {
        if (matcher_(item)) {
            const_cast<Expression* &>(item) = rewriter_(item);
        } else {
            item->accept(this);
        }
    }
}

void RewriteVisitor::visit(SetExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    auto &items = expr->items();
    for (auto &item : items) {
        if (matcher_(item)) {
            const_cast<Expression* &>(item) = rewriter_(item);
        } else {
            item->accept(this);
        }
    }
}

void RewriteVisitor::visit(MapExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    auto &items = expr->items();
    for (auto &pair : items) {
        auto &item = pair.second;
        if (matcher_(item)) {
            const_cast<Expression* &>(item) = rewriter_(item);
        } else {
            item->accept(this);
        }
    }
}

void RewriteVisitor::visit(CaseExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    if (expr->hasCondition()) {
        if (matcher_(expr->condition())) {
            expr->setCondition(rewriter_(expr->condition()));
        } else {
            expr->condition()->accept(this);
        }
    }
    if (expr->hasDefault()) {
        if (matcher_(expr->defaultResult())) {
            expr->setDefault(rewriter_(expr->defaultResult()));
        } else {
            expr->defaultResult()->accept(this);
        }
    }
    auto &cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when;
        auto then = cases[i].then;
        if (matcher_(when)) {
            expr->setWhen(i, rewriter_(when));
        } else {
            when->accept(this);
        }
        if (matcher_(then)) {
            expr->setThen(i, rewriter_(then));
        } else {
            then->accept(this);
        }
    }
}

void RewriteVisitor::visit(ListComprehensionExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    if (matcher_(expr->collection())) {
        expr->setCollection(rewriter_(expr->collection()));
    } else {
        expr->collection()->accept(this);
    }
    if (expr->hasFilter()) {
        if (matcher_(expr->filter())) {
            expr->setFilter(rewriter_(expr->filter()));
        } else {
            expr->filter()->accept(this);
        }
    }
    if (expr->hasMapping()) {
        if (matcher_(expr->mapping())) {
            expr->setMapping(rewriter_(expr->mapping()));
        } else {
            expr->mapping()->accept(this);
        }
    }
}

void RewriteVisitor::visit(PredicateExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    if (matcher_(expr->collection())) {
        expr->setCollection(rewriter_(expr->collection()));
    } else {
        expr->collection()->accept(this);
    }
    if (expr->hasFilter()) {
        if (matcher_(expr->filter())) {
            expr->setFilter(rewriter_(expr->filter()));
        } else {
            expr->filter()->accept(this);
        }
    }
}

void RewriteVisitor::visit(ReduceExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    if (matcher_(expr->initial())) {
        expr->setInitial(rewriter_(expr->initial()));
    } else {
        expr->initial()->accept(this);
    }
    if (matcher_(expr->collection())) {
        expr->setCollection(rewriter_(expr->collection()));
    } else {
        expr->collection()->accept(this);
    }
    if (matcher_(expr->mapping())) {
        expr->setMapping(rewriter_(expr->mapping()));
    } else {
        expr->mapping()->accept(this);
    }
}

void RewriteVisitor::visit(PathBuildExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    auto &items = expr->items();
    for (auto &item : items) {
        if (matcher_(item)) {
            const_cast<Expression* &>(item) = rewriter_(item);
        } else {
            item->accept(this);
        }
    }
}

void RewriteVisitor::visit(AttributeExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    visitBinaryExpr(expr);
}

void RewriteVisitor::visit(ArithmeticExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    visitBinaryExpr(expr);
}

void RewriteVisitor::visit(RelationalExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    visitBinaryExpr(expr);
}

void RewriteVisitor::visit(SubscriptExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }

    visitBinaryExpr(expr);
}

void RewriteVisitor::visit(SubscriptRangeExpression *expr) {
    if (!care(expr->kind())) {
        return;
    }
    if (matcher_(expr->list())) {
        expr->setList(rewriter_(expr->list()));
    } else {
        expr->list()->accept(this);
    }
    if (expr->lo() != nullptr) {
        if (matcher_(expr->lo())) {
            expr->setLo(rewriter_(expr->lo()));
        } else {
            expr->lo()->accept(this);
        }
    }
    if (expr->hi() != nullptr) {
        if (matcher_(expr->hi())) {
            expr->setHi(rewriter_(expr->hi()));
        } else {
            expr->hi()->accept(this);
        }
    }
}

void RewriteVisitor::visitBinaryExpr(BinaryExpression *expr) {
    if (matcher_(expr->left())) {
        expr->setLeft(rewriter_(expr->left()));
    } else {
        expr->left()->accept(this);
    }
    if (matcher_(expr->right())) {
        expr->setRight(rewriter_(expr->right()));
    } else {
        expr->right()->accept(this);
    }
}

bool RewriteVisitor::care(Expression::Kind kind) {
    if (UNLIKELY(!needVisitedTypes_.empty())) {
        for (auto &k : needVisitedTypes_) {
            if (kind == k) {
                return true;
            }
        }
        return false;
    }
    return true;
}

Expression *RewriteVisitor::transform(const Expression *expr, Matcher matcher, Rewriter rewriter) {
    if (matcher(expr)) {
        return rewriter(expr);
    } else {
        RewriteVisitor visitor(std::move(matcher), std::move(rewriter));
        auto exprCopy = expr->clone();
        exprCopy->accept(&visitor);
        return exprCopy;
    }
}

Expression *RewriteVisitor::transform(
    const Expression *expr,
    Matcher matcher,
    Rewriter rewriter,
    const std::unordered_set<Expression::Kind> &needVisitedTypes) {
    if (matcher(expr)) {
        return rewriter(expr);
    } else {
        RewriteVisitor visitor(
            std::move(matcher), std::move(rewriter), std::move(needVisitedTypes));
        auto exprCopy = expr->clone();
        exprCopy->accept(&visitor);
        return exprCopy;
    }
}
}   // namespace graph
}   // namespace nebula
