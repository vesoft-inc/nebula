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
    auto items = expr->items();
    auto found = std::find_if(
        items.cbegin(), items.cend(), [](auto& pair) { return isLabelAttrExpr(pair.second); });
    if (found == items.cend()) {
        std::for_each(items.begin(), items.end(), [this](auto& pair) {
            const_cast<Expression*>(pair.second)->accept(this);
        });
        return;
    }

    std::vector<MapExpression::Item> newItems;
    newItems.reserve(items.size());
    for (auto& pair : items) {
        MapExpression::Item newItem;
        newItem.first.reset(new std::string(*pair.first));
        if (isLabelAttrExpr(pair.second)) {
            auto symExpr = static_cast<const LabelAttributeExpression*>(pair.second);
            newItem.second.reset(createExpr(symExpr));
        } else {
            newItem.second = Expression::decode(pair.second->encode());
            newItem.second->accept(this);
        }
        newItems.emplace_back(std::move(newItem));
    }
    expr->setItems(std::move(newItems));
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
    const std::vector<const Expression*>& exprs) {
    std::vector<std::unique_ptr<Expression>> newExprs;

    auto found = std::find_if(exprs.cbegin(), exprs.cend(), isLabelAttrExpr);
    if (found == exprs.cend()) {
        std::for_each(exprs.cbegin(), exprs.cend(), [this](auto expr) {
            const_cast<Expression*>(expr)->accept(this);
        });
        return newExprs;
    }

    newExprs.reserve(exprs.size());
    for (auto item : exprs) {
        if (isLabelAttrExpr(item)) {
            auto symExpr = static_cast<const LabelAttributeExpression*>(item);
            newExprs.emplace_back(createExpr(symExpr));
        } else {
            auto newExpr = Expression::decode(item->encode());
            newExpr->accept(this);
            newExprs.emplace_back(std::move(newExpr));
        }
    }
    return newExprs;
}

Expression* RewriteLabelAttrVisitor::createExpr(const LabelAttributeExpression* expr) {
    auto leftName = new std::string(*expr->left()->name());
    auto rightName = new std::string(*expr->right()->name());
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
