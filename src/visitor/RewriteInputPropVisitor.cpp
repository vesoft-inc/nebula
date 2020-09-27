/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteInputPropVisitor.h"

namespace nebula {
namespace graph {

void RewriteInputPropVisitor::visit(UUIDExpression* expr) {
    reportError(expr);
}

void RewriteInputPropVisitor::visit(VariableExpression* expr) {
    reportError(expr);
}

void RewriteInputPropVisitor::visit(VersionedVariableExpression* expr) {
    reportError(expr);
}

void RewriteInputPropVisitor::visit(LabelAttributeExpression* expr) {
    reportError(expr);
}

void RewriteInputPropVisitor::visit(LabelExpression* expr) {
    reportError(expr);
}

void RewriteInputPropVisitor::visit(AttributeExpression* expr) {
    reportError(expr);
}

void RewriteInputPropVisitor::visit(SubscriptExpression* expr) {
    reportError(expr);
}

void RewriteInputPropVisitor::visit(ConstantExpression* expr) {
    UNUSED(expr);
}

void RewriteInputPropVisitor::visit(VertexExpression* expr) {
    UNUSED(expr);
}

void RewriteInputPropVisitor::visit(EdgeExpression* expr) {
    UNUSED(expr);
}

void RewriteInputPropVisitor::visit(InputPropertyExpression* expr) {
    UNUSED(expr);
}

void RewriteInputPropVisitor::visit(ArithmeticExpression* expr) {
    visitBinaryExpr(expr);
}

void RewriteInputPropVisitor::visit(RelationalExpression* expr) {
    visitBinaryExpr(expr);
}

void RewriteInputPropVisitor::visit(LogicalExpression* expr) {
    visitBinaryExpr(expr);
}

void RewriteInputPropVisitor::visit(UnaryExpression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot: {
            visitUnaryExpr(expr);
            break;
        }
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr: {
            reportError(expr);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid kind " << expr->kind();
        }
    }
}

void RewriteInputPropVisitor::visit(TagPropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(SourcePropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(DestPropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(EdgePropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(EdgeSrcIdExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(EdgeTypeExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(EdgeRankExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(EdgeDstIdExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(VariablePropertyExpression* expr) {
    visitVertexEdgePropExpr(expr);
}

void RewriteInputPropVisitor::visit(ListExpression* expr) {
    auto items = std::move(*expr).get();
    for (auto iter = items.begin(); iter < items.end(); ++iter) {
        iter->get()->accept(this);
        if (ok()) {
            *iter = std::move(result_);
        }
    }
    expr->setItems(std::move(items));
}

void RewriteInputPropVisitor::visit(SetExpression* expr) {
    auto items = std::move(*expr).get();
    for (auto iter = items.begin(); iter < items.end(); ++iter) {
        iter->get()->accept(this);
        if (ok()) {
            *iter = std::move(result_);
        }
    }
    expr->setItems(std::move(items));
}

void RewriteInputPropVisitor::visit(MapExpression* expr) {
    auto items = std::move(*expr).get();
    for (auto iter = items.begin(); iter < items.end(); ++iter) {
        iter->second.get()->accept(this);
        if (ok()) {
            *iter = std::make_pair(std::move(iter->first), std::move(result_));
        }
    }
    expr->setItems(std::move(items));
}

void RewriteInputPropVisitor::visit(FunctionCallExpression *expr) {
    auto* argList = const_cast<ArgumentList*>(expr->args());
    auto args = argList->moveArgs();
    for (auto iter = args.begin(); iter < args.end(); ++iter) {
        iter->get()->accept(this);
        if (ok()) {
            *iter = std::move(result_);
        }
    }
    argList->setArgs(std::move(args));
}

void RewriteInputPropVisitor::visit(TypeCastingExpression * expr) {
    expr->operand()->accept(this);
    if (ok()) {
        expr->setOperand(result_.release());
    }
}

void RewriteInputPropVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (ok()) {
        expr->setLeft(result_.release());
    }
    expr->right()->accept(this);
    if (ok()) {
        expr->setRight(result_.release());
    }
}

void RewriteInputPropVisitor::visitUnaryExpr(UnaryExpression* expr) {
    expr->operand()->accept(this);
    if (ok()) {
        expr->setOperand(result_.release());
    }
}

void RewriteInputPropVisitor::visitVertexEdgePropExpr(PropertyExpression * expr) {
    PropertyExpression* propExpr = nullptr;
    switch (expr->kind()) {
        case Expression::Kind::kTagProperty: {
            propExpr = static_cast<TagPropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kSrcProperty: {
            propExpr = static_cast<SourcePropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kDstProperty: {
            propExpr = static_cast<DestPropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeProperty: {
            propExpr = static_cast<EdgePropertyExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeSrc: {
            propExpr = static_cast<EdgeSrcIdExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeType: {
            propExpr = static_cast<EdgeTypeExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeRank: {
            propExpr = static_cast<EdgeRankExpression*>(expr);
            break;
        }
        case Expression::Kind::kEdgeDst: {
            propExpr = static_cast<EdgeDstIdExpression*>(expr);
            break;
        }
        case Expression::Kind::kVarProperty: {
            propExpr = static_cast<VariablePropertyExpression*>(expr);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid Kind " << expr->kind();
        }
    }
    auto found = propExprColMap_.find(propExpr->toString());
    DCHECK(found != propExprColMap_.end());
    auto alias = new std::string(*(found->second->alias()));
    result_ = std::make_unique<InputPropertyExpression>(alias);
}

void RewriteInputPropVisitor::reportError(const Expression *expr) {
    std::stringstream ss;
    ss << "Not supported expression `" << expr->toString() << "' for RewriteInputProps.";
    status_ = Status::SemanticError(ss.str());
}

}  // namespace graph
}  // namespace nebula
