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

void RewriteInputPropVisitor::visit(ColumnExpression* expr) {
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
    auto &operands = expr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        operands[i]->accept(this);
        if (ok()) {
            expr->setOperand(i, result_.release());
        }
    }
}

void RewriteInputPropVisitor::visit(UnaryExpression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kIsNull:
        case Expression::Kind::kIsNotNull: {
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
    const auto& items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (ok()) {
            expr->setItem(i, std::move(result_));
        }
    }
}

void RewriteInputPropVisitor::visit(SetExpression* expr) {
    const auto& items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (ok()) {
            expr->setItem(i, std::move(result_));
        }
    }
}

void RewriteInputPropVisitor::visit(MapExpression* expr) {
    const auto& items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i].second->accept(this);
        if (ok()) {
            auto key = std::make_unique<std::string>(*items[i].first);
            expr->setItem(i, {std::move(key), std::move(result_)});
        }
    }
}

void RewriteInputPropVisitor::visit(FunctionCallExpression* expr) {
    auto& args = expr->args()->args();
    for (size_t i = 0; i < args.size(); ++i) {
        args[i]->accept(this);
        if (ok()) {
            expr->args()->setArg(i, std::move(result_));
        }
    }
}

void RewriteInputPropVisitor::visit(AggregateExpression* expr) {
    auto* arg = expr->arg();
    arg->accept(this);
    if (ok()) {
        expr->setArg(std::move(result_).release());
    }
}

void RewriteInputPropVisitor::visit(TypeCastingExpression* expr) {
    expr->operand()->accept(this);
    if (ok()) {
        expr->setOperand(result_.release());
    }
}

void RewriteInputPropVisitor::visit(CaseExpression* expr) {
    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        if (ok()) {
            expr->setCondition(result_.release());
        }
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        if (ok()) {
            expr->setDefault(result_.release());
        }
    }
    for (size_t i = 0; i < expr->cases().size(); ++i) {
        const auto& whenThen = expr->cases()[i];
        whenThen.when->accept(this);
        if (ok()) {
            expr->setWhen(i, result_.release());
        }
        whenThen.then->accept(this);
        if (ok()) {
            expr->setThen(i, result_.release());
        }
    }
}

void RewriteInputPropVisitor::visit(ListComprehensionExpression *expr) {
    expr->collection()->accept(this);
    if (ok()) {
        expr->setCollection(result_.release());
    }
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (ok()) {
            expr->setFilter(result_.release());
        }
    }
    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
        if (ok()) {
            expr->setMapping(result_.release());
        }
    }
}

void RewriteInputPropVisitor::visitBinaryExpr(BinaryExpression* expr) {
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

void RewriteInputPropVisitor::visitVertexEdgePropExpr(PropertyExpression* expr) {
    auto found = propExprColMap_.find(expr->toString());
    DCHECK(found != propExprColMap_.end());
    auto alias = new std::string(*(found->second->alias()));
    result_ = std::make_unique<InputPropertyExpression>(alias);
}

void RewriteInputPropVisitor::reportError(const Expression* expr) {
    std::stringstream ss;
    ss << "Not supported expression `" << expr->toString() << "' for RewriteInputProps.";
    status_ = Status::SemanticError(ss.str());
}

void RewriteInputPropVisitor::visit(PathBuildExpression* expr) {
    const auto& items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (ok()) {
            expr->setItem(i, std::move(result_));
        }
    }
}

void RewriteInputPropVisitor::visit(PredicateExpression* expr) {
    expr->collection()->accept(this);
    if (ok()) {
        expr->setCollection(result_.release());
    }
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (ok()) {
            expr->setFilter(result_.release());
        }
    }
}

void RewriteInputPropVisitor::visit(ReduceExpression* expr) {
    expr->initial()->accept(this);
    if (ok()) {
        expr->setInitial(result_.release());
    }
    expr->collection()->accept(this);
    if (ok()) {
        expr->setCollection(result_.release());
    }
    expr->mapping()->accept(this);
    if (ok()) {
        expr->setMapping(result_.release());
    }
}

}   // namespace graph
}   // namespace nebula
