/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteSymExprVisitor.h"

namespace nebula {
namespace graph {

RewriteSymExprVisitor::RewriteSymExprVisitor(const std::string &sym, bool isEdge)
    : sym_(sym), isEdge_(isEdge) {}

void RewriteSymExprVisitor::visit(ConstantExpression *expr) {
    UNUSED(expr);
    expr_.reset();
}

void RewriteSymExprVisitor::visit(UnaryExpression *expr) {
    expr->operand()->accept(this);
    if (expr_) {
        expr->setOperand(expr_.release());
    }
}

void RewriteSymExprVisitor::visit(TypeCastingExpression *expr) {
    expr->operand()->accept(this);
    if (expr_) {
        expr->setOperand(expr_.release());
    }
}

void RewriteSymExprVisitor::visit(LabelExpression *expr) {
    if (isEdge_) {
        expr_ = std::make_unique<EdgePropertyExpression>(new std::string(sym_),
                                                         new std::string(*expr->name()));
    } else {
        expr_ = std::make_unique<SourcePropertyExpression>(new std::string(sym_),
                                                           new std::string(*expr->name()));
    }
}

void RewriteSymExprVisitor::visit(LabelAttributeExpression *expr) {
    if (isEdge_) {
        expr_ = std::make_unique<EdgePropertyExpression>(
            new std::string(*expr->left()->name()),
            new std::string(expr->right()->value().getStr()));
        hasWrongType_ = false;
    } else {
        hasWrongType_ = true;
        expr_.reset();
    }
}

void RewriteSymExprVisitor::visit(ArithmeticExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteSymExprVisitor::visit(RelationalExpression *expr) {
    visitBinaryExpr(expr);
}

void RewriteSymExprVisitor::visit(SubscriptExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(AttributeExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(LogicalExpression *expr) {
    auto &operands = expr->operands();
    for (auto i = 0u; i < operands.size(); i++) {
        operands[i]->accept(this);
        if (expr_) {
            expr->setOperand(i, expr_.release());
        }
    }
}

// function call
void RewriteSymExprVisitor::visit(FunctionCallExpression *expr) {
    const auto &args = expr->args()->args();
    for (size_t i = 0; i < args.size(); ++i) {
        auto &arg = args[i];
        arg->accept(this);
        if (expr_) {
            expr->args()->setArg(i, std::move(expr_));
        }
    }
}

void RewriteSymExprVisitor::visit(AggregateExpression *expr) {
    auto* arg = expr->arg();
    arg->accept(this);
    if (expr_) {
        expr->setArg(std::move(expr_).release());
    }
}

void RewriteSymExprVisitor::visit(UUIDExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

// variable expression
void RewriteSymExprVisitor::visit(VariableExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(VersionedVariableExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

// container expression
void RewriteSymExprVisitor::visit(ListExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (expr_) {
            expr->setItem(i, std::move(expr_));
        }
    }
}

void RewriteSymExprVisitor::visit(SetExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (expr_) {
            expr->setItem(i, std::move(expr_));
        }
    }
}

void RewriteSymExprVisitor::visit(MapExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i].second->accept(this);
        if (expr_) {
            auto key = std::make_unique<std::string>(*items[i].first);
            expr->setItem(i, {std::move(key), std::move(expr_)});
        }
    }
}

// property Expression
void RewriteSymExprVisitor::visit(TagPropertyExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(EdgePropertyExpression *expr) {
    UNUSED(expr);
    if (!isEdge_) {
        hasWrongType_ = true;
    }
}

void RewriteSymExprVisitor::visit(InputPropertyExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(VariablePropertyExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(DestPropertyExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(SourcePropertyExpression *expr) {
    UNUSED(expr);
    if (isEdge_) {
        hasWrongType_ = true;
    }
}

void RewriteSymExprVisitor::visit(EdgeSrcIdExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(EdgeTypeExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(EdgeRankExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(EdgeDstIdExpression *expr) {
    UNUSED(expr);
    hasWrongType_ = true;
}

void RewriteSymExprVisitor::visit(VertexExpression *expr) {
    UNUSED(expr);
    expr_.reset();
}

void RewriteSymExprVisitor::visit(EdgeExpression *expr) {
    UNUSED(expr);
    expr_.reset();
}

void RewriteSymExprVisitor::visit(ColumnExpression *expr) {
    UNUSED(expr);
    expr_.reset();
}

void RewriteSymExprVisitor::visit(CaseExpression *expr) {
    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        if (expr_) {
            expr->setCondition(expr_.release());
        }
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        if (expr_) {
            expr->setDefault(expr_.release());
        }
    }
    auto &cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when.get();
        auto then = cases[i].then.get();
        when->accept(this);
        if (expr_) {
            expr->setWhen(i, expr_.release());
        }
        then->accept(this);
        if (expr_) {
            expr->setThen(i, expr_.release());
        }
    }
}

void RewriteSymExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    expr->left()->accept(this);
    if (expr_) {
        expr->setLeft(expr_.release());
    }
    expr->right()->accept(this);
    if (expr_) {
        expr->setRight(expr_.release());
    }
}

void RewriteSymExprVisitor::visit(PathBuildExpression *expr) {
    const auto &items = expr->items();
    for (size_t i = 0; i < items.size(); ++i) {
        items[i]->accept(this);
        if (expr_) {
            expr->setItem(i, std::move(expr_));
        }
    }
}

void RewriteSymExprVisitor::visit(ListComprehensionExpression *expr) {
    expr->collection()->accept(this);
    if (expr_) {
        expr->setCollection(expr_.release());
    }
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (expr_) {
            expr->setFilter(expr_.release());
        }
    }
    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
        if (expr_) {
            expr->setMapping(expr_.release());
        }
    }
}

void RewriteSymExprVisitor::visit(PredicateExpression *expr) {
    expr->collection()->accept(this);
    if (expr_) {
        expr->setCollection(expr_.release());
    }
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (expr_) {
            expr->setFilter(expr_.release());
        }
    }
}

void RewriteSymExprVisitor::visit(ReduceExpression *expr) {
    expr->initial()->accept(this);
    if (expr_) {
        expr->setInitial(expr_.release());
    }
    expr->collection()->accept(this);
    if (expr_) {
        expr->setCollection(expr_.release());
    }
    expr->mapping()->accept(this);
    if (expr_) {
        expr->setMapping(expr_.release());
    }
}

void RewriteSymExprVisitor::visit(SubscriptRangeExpression *expr) {
    expr->list()->accept(this);
    if (expr_) {
        expr->setList(expr_.release());
    }
    if (expr->lo() != nullptr) {
        expr->lo()->accept(this);
        if (expr_) {
            expr->setLo(expr_.release());
        }
    }
    if (expr->hi() != nullptr) {
        expr->hi()->accept(this);
        if (expr_) {
            expr->setHi(expr_.release());
        }
    }
}

}   // namespace graph
}   // namespace nebula
