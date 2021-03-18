/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteLabelAttrVisitor.h"

#include "common/base/Logging.h"

namespace nebula {
namespace graph {

RewriteLabelAttrVisitor::RewriteLabelAttrVisitor(bool isTag) : isTag_(isTag) {}

void RewriteLabelAttrVisitor::visit(TypeCastingExpression* expr) {
    if (isLabelAttrExpr(expr->operand())) {
        auto operand = static_cast<LabelAttributeExpression*>(expr->operand());
        expr->setOperand(createExpr(operand));
    } else {
        expr->operand()->accept(this);
    }
}

void RewriteLabelAttrVisitor::visit(UnaryExpression* expr) {
    if (isLabelAttrExpr(expr->operand())) {
        auto operand = static_cast<LabelAttributeExpression*>(expr->operand());
        expr->setOperand(createExpr(operand));
    } else {
        expr->operand()->accept(this);
    }
}

void RewriteLabelAttrVisitor::visit(FunctionCallExpression* expr) {
    for (auto& arg : expr->args()->args()) {
        if (isLabelAttrExpr(arg.get())) {
            auto newArg = static_cast<LabelAttributeExpression*>(arg.get());
            arg.reset(createExpr(newArg));
        } else {
            arg->accept(this);
        }
    }
}

void RewriteLabelAttrVisitor::visit(ListExpression* expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}

void RewriteLabelAttrVisitor::visit(SetExpression* expr) {
    auto newItems = rewriteExprList(expr->items());
    if (!newItems.empty()) {
        expr->setItems(std::move(newItems));
    }
}

void RewriteLabelAttrVisitor::visit(MapExpression* expr) {
    auto& items = expr->items();
    auto found = std::find_if(items.cbegin(), items.cend(), [](auto& pair) {
        return isLabelAttrExpr(pair.second.get());
    });
    if (found == items.cend()) {
        std::for_each(
            items.begin(), items.end(), [this](auto& pair) { pair.second->accept(this); });
        return;
    }

    std::vector<MapExpression::Item> newItems;
    newItems.reserve(items.size());
    for (auto& pair : items) {
        MapExpression::Item newItem;
        newItem.first.reset(new std::string(*pair.first));
        if (isLabelAttrExpr(pair.second.get())) {
            auto symExpr = static_cast<const LabelAttributeExpression*>(pair.second.get());
            newItem.second.reset(createExpr(symExpr));
        } else {
            newItem.second = pair.second->clone();
            newItem.second->accept(this);
        }
        newItems.emplace_back(std::move(newItem));
    }
    expr->setItems(std::move(newItems));
}

void RewriteLabelAttrVisitor::visit(CaseExpression* expr) {
    if (expr->hasCondition()) {
        if (isLabelAttrExpr(expr->condition())) {
            auto newExpr = static_cast<LabelAttributeExpression*>(expr->condition());
            expr->setCondition(createExpr(newExpr));
        } else {
            expr->condition()->accept(this);
        }
    }
    if (expr->hasDefault()) {
        if (isLabelAttrExpr(expr->defaultResult())) {
            auto newExpr = static_cast<LabelAttributeExpression*>(expr->defaultResult());
            expr->setDefault(createExpr(newExpr));
        } else {
            expr->defaultResult()->accept(this);
        }
    }
    auto& cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when.get();
        auto then = cases[i].then.get();
        if (isLabelAttrExpr(when)) {
            auto newExpr = static_cast<LabelAttributeExpression*>(when);
            expr->setWhen(i, createExpr(newExpr));
        } else {
            when->accept(this);
        }
        if (isLabelAttrExpr(then)) {
            auto newExpr = static_cast<LabelAttributeExpression*>(then);
            expr->setThen(i, createExpr(newExpr));
        } else {
            then->accept(this);
        }
    }
}

void RewriteLabelAttrVisitor::visit(ListComprehensionExpression* expr) {
    if (isLabelAttrExpr(expr->collection())) {
        auto newExpr = static_cast<LabelAttributeExpression*>(expr->collection());
        expr->setCollection(createExpr(newExpr));
    } else {
        expr->collection()->accept(this);
    }
    if (expr->hasFilter()) {
        if (isLabelAttrExpr(expr->filter())) {
            auto newExpr = static_cast<LabelAttributeExpression*>(expr->filter());
            expr->setFilter(createExpr(newExpr));
        } else {
            expr->filter()->accept(this);
        }
    }
    if (expr->hasMapping()) {
        if (isLabelAttrExpr(expr->mapping())) {
            auto newExpr = static_cast<LabelAttributeExpression*>(expr->mapping());
            expr->setMapping(createExpr(newExpr));
        } else {
            expr->mapping()->accept(this);
        }
    }
}

void RewriteLabelAttrVisitor::visit(PredicateExpression* expr) {
    if (isLabelAttrExpr(expr->collection())) {
        auto newExpr = static_cast<LabelAttributeExpression*>(expr->collection());
        expr->setCollection(createExpr(newExpr));
    } else {
        expr->collection()->accept(this);
    }
    if (expr->hasFilter()) {
        if (isLabelAttrExpr(expr->filter())) {
            auto newExpr = static_cast<LabelAttributeExpression*>(expr->filter());
            expr->setFilter(createExpr(newExpr));
        } else {
            expr->filter()->accept(this);
        }
    }
}

void RewriteLabelAttrVisitor::visit(ReduceExpression* expr) {
    if (isLabelAttrExpr(expr->initial())) {
        auto newExpr = static_cast<LabelAttributeExpression*>(expr->initial());
        expr->setCollection(createExpr(newExpr));
    } else {
        expr->initial()->accept(this);
    }
    if (isLabelAttrExpr(expr->collection())) {
        auto newExpr = static_cast<LabelAttributeExpression*>(expr->collection());
        expr->setCollection(createExpr(newExpr));
    } else {
        expr->collection()->accept(this);
    }
    if (isLabelAttrExpr(expr->mapping())) {
        auto newExpr = static_cast<LabelAttributeExpression*>(expr->mapping());
        expr->setMapping(createExpr(newExpr));
    } else {
        expr->mapping()->accept(this);
    }
}

void RewriteLabelAttrVisitor::visitBinaryExpr(BinaryExpression* expr) {
    if (isLabelAttrExpr(expr->left())) {
        auto left = static_cast<const LabelAttributeExpression*>(expr->left());
        expr->setLeft(createExpr(left));
    } else {
        expr->left()->accept(this);
    }
    if (isLabelAttrExpr(expr->right())) {
        auto right = static_cast<const LabelAttributeExpression*>(expr->right());
        expr->setRight(createExpr(right));
    } else {
        expr->right()->accept(this);
    }
}

std::vector<std::unique_ptr<Expression>> RewriteLabelAttrVisitor::rewriteExprList(
    const std::vector<std::unique_ptr<Expression>>& exprs) {
    std::vector<std::unique_ptr<Expression>> newExprs;

    auto found = std::find_if(
        exprs.cbegin(), exprs.cend(), [](auto& expr) { return isLabelAttrExpr(expr.get()); });
    if (found == exprs.cend()) {
        std::for_each(exprs.cbegin(), exprs.cend(), [this](auto& expr) { expr->accept(this); });
        return newExprs;
    }

    newExprs.reserve(exprs.size());
    for (auto& item : exprs) {
        if (isLabelAttrExpr(item.get())) {
            auto symExpr = static_cast<const LabelAttributeExpression*>(item.get());
            newExprs.emplace_back(createExpr(symExpr));
        } else {
            auto newExpr = item->clone();
            newExpr->accept(this);
            newExprs.emplace_back(std::move(newExpr));
        }
    }
    return newExprs;
}

Expression* RewriteLabelAttrVisitor::createExpr(const LabelAttributeExpression* expr) {
    auto leftName = new std::string(*expr->left()->name());
    const auto &value = expr->right()->value();
    auto rightName = new std::string(value.getStr());
    if (isTag_) {
        return new TagPropertyExpression(leftName, rightName);
    }
    return new EdgePropertyExpression(leftName, rightName);
}

bool RewriteLabelAttrVisitor::isLabelAttrExpr(const Expression* expr) {
    return expr->kind() == Expression::Kind::kLabelAttribute;
}

}   // namespace graph
}   // namespace nebula
