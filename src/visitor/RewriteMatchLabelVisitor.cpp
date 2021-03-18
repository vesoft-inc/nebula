/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {

void RewriteMatchLabelVisitor::visit(TypeCastingExpression *expr) {
    if (isLabel(expr->operand())) {
        expr->setOperand(rewriter_(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}


void RewriteMatchLabelVisitor::visit(UnaryExpression *expr) {
    if (isLabel(expr->operand())) {
        expr->setOperand(rewriter_(expr->operand()));
    } else {
        expr->operand()->accept(this);
    }
}


void RewriteMatchLabelVisitor::visit(FunctionCallExpression *expr) {
    for (auto &arg : expr->args()->args()) {
        if (isLabel(arg.get())) {
            arg.reset(rewriter_(arg.get()));
        } else {
            arg->accept(this);
        }
    }
}

void RewriteMatchLabelVisitor::visit(AggregateExpression *expr) {
    auto arg = expr->arg();
    if (isLabel(arg)) {
        expr->setArg(rewriter_(arg));
    } else {
        arg->accept(this);
    }
}

void RewriteMatchLabelVisitor::visit(AttributeExpression *expr) {
    if (isLabel(expr->left())) {
        expr->setLeft(rewriter_(expr->left()));
    } else {
        expr->left()->accept(this);
    }
}


void RewriteMatchLabelVisitor::visit(ListExpression *expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}


void RewriteMatchLabelVisitor::visit(SetExpression *expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}


void RewriteMatchLabelVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    auto iter = std::find_if(items.cbegin(), items.cend(), [] (auto &pair) {
        return isLabel(pair.second.get());
    });
    if (iter == items.cend()) {
        return;
    }

    std::vector<MapExpression::Item> newItems;
    newItems.reserve(items.size());
    for (auto &pair : items) {
        MapExpression::Item newItem;
        newItem.first.reset(new std::string(*pair.first));
        if (isLabel(pair.second.get())) {
            newItem.second.reset(rewriter_(pair.second.get()));
        } else {
            newItem.second = pair.second->clone();
            newItem.second->accept(this);
        }
        newItems.emplace_back(std::move(newItem));
    }

    expr->setItems(std::move(newItems));
}

void RewriteMatchLabelVisitor::visit(CaseExpression *expr) {
    if (expr->hasCondition()) {
        if (isLabel(expr->condition())) {
            expr->setCondition(rewriter_(expr->condition()));
        } else {
            expr->condition()->accept(this);
        }
    }
    if (expr->hasDefault()) {
        if (isLabel(expr->defaultResult())) {
            expr->setDefault(rewriter_(expr->defaultResult()));
        } else {
            expr->defaultResult()->accept(this);
        }
    }
    auto &cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when.get();
        auto then = cases[i].then.get();
        if (isLabel(when)) {
            expr->setWhen(i, rewriter_(when));
        } else {
            when->accept(this);
        }
        if (isLabel(then)) {
            expr->setThen(i, rewriter_(then));
        } else {
            then->accept(this);
        }
    }
}

void RewriteMatchLabelVisitor::visit(ListComprehensionExpression *expr) {
    if (isLabel(expr->collection())) {
        expr->setCollection(rewriter_(expr->collection()));
    } else {
        expr->collection()->accept(this);
    }
    if (expr->hasFilter()) {
        if (isLabel(expr->filter())) {
            expr->setFilter(rewriter_(expr->filter()));
        } else {
            expr->filter()->accept(this);
        }
    }
    if (expr->hasMapping()) {
        if (isLabel(expr->mapping())) {
            expr->setMapping(rewriter_(expr->mapping()));
        } else {
            expr->mapping()->accept(this);
        }
    }
}

void RewriteMatchLabelVisitor::visit(PredicateExpression *expr) {
    if (isLabel(expr->collection())) {
        expr->setCollection(rewriter_(expr->collection()));
    } else {
        expr->collection()->accept(this);
    }
    if (expr->hasFilter()) {
        if (isLabel(expr->filter())) {
            expr->setFilter(rewriter_(expr->filter()));
        } else {
            expr->filter()->accept(this);
        }
    }
}

void RewriteMatchLabelVisitor::visit(ReduceExpression *expr) {
    if (isLabel(expr->initial())) {
        expr->setInitial(rewriter_(expr->initial()));
    } else {
        expr->initial()->accept(this);
    }
    if (isLabel(expr->collection())) {
        expr->setCollection(rewriter_(expr->collection()));
    } else {
        expr->collection()->accept(this);
    }
    if (isLabel(expr->mapping())) {
        expr->setMapping(rewriter_(expr->mapping()));
    } else {
        expr->mapping()->accept(this);
    }
}

void RewriteMatchLabelVisitor::visitBinaryExpr(BinaryExpression *expr) {
    if (isLabel(expr->left())) {
        expr->setLeft(rewriter_(expr->left()));
    } else {
        expr->left()->accept(this);
    }
    if (isLabel(expr->right())) {
        expr->setRight(rewriter_(expr->right()));
    } else {
        expr->right()->accept(this);
    }
}


std::vector<std::unique_ptr<Expression>>
RewriteMatchLabelVisitor::rewriteExprList(const std::vector<std::unique_ptr<Expression>> &list) {
    std::vector<std::unique_ptr<Expression>> newList;
    auto iter = std::find_if(list.cbegin(), list.cend(), [] (auto &expr) {
            return isLabel(expr.get());
    });
    if (iter == list.cend()) {
        std::for_each(list.cbegin(), list.cend(), [this] (auto &expr) {
            const_cast<Expression*>(expr.get())->accept(this);
        });
        return newList;
    }

    newList.reserve(list.size());
    for (auto &expr : list) {
        if (isLabel(expr.get())) {
            newList.emplace_back(rewriter_(expr.get()));
        } else {
            auto newExpr = expr->clone();
            newExpr->accept(this);
            newList.emplace_back(std::move(newExpr));
        }
    }

    return newList;
}

}   // namespace graph
}   // namespace nebula
