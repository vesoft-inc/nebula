/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/function/FunctionManager.h"

#include "visitor/FoldConstantExprVisitor.h"

#include "context/QueryExpressionContext.h"
#include "util/ExpressionUtils.h"
namespace nebula {
namespace graph {

void FoldConstantExprVisitor::visit(ConstantExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = true;
}

void FoldConstantExprVisitor::visit(UnaryExpression *expr) {
    if (!isConstant(expr->operand())) {
        expr->operand()->accept(this);
        if (canBeFolded_) {
            expr->setOperand(fold(expr->operand()));
            if (!ok()) return;
        }
    } else {
        canBeFolded_ = expr->kind() == Expression::Kind::kUnaryNegate ||
                       expr->kind() == Expression::Kind::kUnaryPlus;
    }
}

void FoldConstantExprVisitor::visit(TypeCastingExpression *expr) {
    if (!isConstant(expr->operand())) {
        expr->operand()->accept(this);
        if (canBeFolded_) {
            expr->setOperand(fold(expr->operand()));
            if (!ok()) return;
        }
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
    auto &operands = expr->operands();
    auto foldable = true;
    // auto shortCircuit = false;
    for (auto i = 0u; i < operands.size(); i++) {
        auto *operand = operands[i];
        operand->accept(this);
        if (canBeFolded_) {
            auto *newExpr = fold(operand);
            if (!ok()) return;
            expr->setOperand(i, newExpr);
            /*
            if (newExpr->value().isBool()) {
                auto value = newExpr->value().getBool();
                if ((value && expr->kind() == Expression::Kind::kLogicalOr) ||
                        (!value && expr->kind() == Expression::Kind::kLogicalAnd)) {
                    shortCircuit = true;
                    break;
                }
            }
            */
        } else {
            foldable = false;
        }
    }
    // canBeFolded_ = foldable || shortCircuit;
    canBeFolded_ = foldable;
}

// function call
void FoldConstantExprVisitor::visit(FunctionCallExpression *expr) {
    bool canBeFolded = true;
    for (auto &arg : expr->args()->args()) {
        if (!isConstant(arg)) {
            arg->accept(this);
            if (canBeFolded_) {
                arg = fold(arg);
                if (!ok()) return;
            } else {
                canBeFolded = false;
            }
        }
    }
    auto result = FunctionManager::getIsPure(expr->name(), expr->args()->args().size());
    if (!result.ok()) {
        canBeFolded = false;
    } else if (!result.value()) {
        // stateful so can't fold
        canBeFolded = false;
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(AggregateExpression *expr) {
    // TODO : impl AggExpr foldConstantExprVisitor
    if (!isConstant(expr->arg())) {
        expr->arg()->accept(this);
        if (canBeFolded_) {
            expr->setArg(fold(expr->arg()));
            if (!ok()) return;
        }
    }
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
        auto item = items[i];
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (canBeFolded_) {
            expr->setItem(i, fold(item));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(SetExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto item = items[i];
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (canBeFolded_) {
            expr->setItem(i, fold(item));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(MapExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto &pair = items[i];
        auto item = const_cast<Expression *>(pair.second);
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (canBeFolded_) {
            auto val = fold(item);
            if (!ok()) return;
            expr->setItem(i, std::make_pair(pair.first, std::move(val)));
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

// case Expression
void FoldConstantExprVisitor::visit(CaseExpression *expr) {
    bool canBeFolded = true;
    if (expr->hasCondition() && !isConstant(expr->condition())) {
        expr->condition()->accept(this);
        if (canBeFolded_) {
            expr->setCondition(fold(expr->condition()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    if (expr->hasDefault() && !isConstant(expr->defaultResult())) {
        expr->defaultResult()->accept(this);
        if (canBeFolded_) {
            expr->setDefault(fold(expr->defaultResult()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    auto &cases = expr->cases();
    for (size_t i = 0; i < cases.size(); ++i) {
        auto when = cases[i].when;
        auto then = cases[i].then;
        if (!isConstant(when)) {
            when->accept(this);
            if (canBeFolded_) {
                expr->setWhen(i, fold(when));
                if (!ok()) return;
            } else {
                canBeFolded = false;
            }
        }
        if (!isConstant(then)) {
            then->accept(this);
            if (canBeFolded_) {
                expr->setThen(i, fold(then));
                if (!ok()) return;
            } else {
                canBeFolded = false;
            }
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

void FoldConstantExprVisitor::visit(ColumnExpression *expr) {
    UNUSED(expr);
    canBeFolded_ = false;
}

void FoldConstantExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    bool leftCanBeFolded = true, rightCanBeFolded = true;
    if (!isConstant(expr->left())) {
        expr->left()->accept(this);
        leftCanBeFolded = canBeFolded_;
        if (leftCanBeFolded) {
            expr->setLeft(fold(expr->left()));
            if (!ok()) return;
        }
    }
    if (!isConstant(expr->right())) {
        expr->right()->accept(this);
        rightCanBeFolded = canBeFolded_;
        if (rightCanBeFolded) {
            expr->setRight(fold(expr->right()));
            if (!ok()) return;
        }
    }
    canBeFolded_ = leftCanBeFolded && rightCanBeFolded;
}

Expression *FoldConstantExprVisitor::fold(Expression *expr) {
    QueryExpressionContext ctx;
    auto value = expr->eval(ctx(nullptr));
    if (value.type() == Value::Type::NULLVALUE) {
        switch (value.getNull()) {
            case NullType::DIV_BY_ZERO:
                canBeFolded_ = false;
                status_ = Status::Error("/ by zero");
                break;
            case NullType::ERR_OVERFLOW:
                canBeFolded_ = false;
                status_ = Status::Error("result of %s cannot be represented as an integer",
                                        expr->toString().c_str());
                break;
            default:
                break;
        }
    } else {
        status_ = Status::OK();
    }
    return ConstantExpression::make(pool_, value);
}

void FoldConstantExprVisitor::visit(PathBuildExpression *expr) {
    auto &items = expr->items();
    bool canBeFolded = true;
    for (size_t i = 0; i < items.size(); ++i) {
        auto item = items[i];
        if (isConstant(item)) {
            continue;
        }
        item->accept(this);
        if (!canBeFolded_) {
            canBeFolded = false;
            continue;
        }
        expr->setItem(i, fold(item));
        if (!ok()) return;
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(ListComprehensionExpression *expr) {
    bool canBeFolded = true;
    if (!isConstant(expr->collection())) {
        expr->collection()->accept(this);
        if (canBeFolded_) {
            expr->setCollection(fold(expr->collection()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    if (expr->hasFilter() && !isConstant(expr->filter())) {
        expr->filter()->accept(this);
        if (canBeFolded_) {
            expr->setFilter(fold(expr->filter()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    if (expr->hasMapping() && !isConstant(expr->mapping())) {
        expr->mapping()->accept(this);
        if (canBeFolded_) {
            expr->setMapping(fold(expr->mapping()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(PredicateExpression *expr) {
    bool canBeFolded = true;
    if (!isConstant(expr->collection())) {
        expr->collection()->accept(this);
        if (canBeFolded_) {
            expr->setCollection(fold(expr->collection()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    if (expr->hasFilter()) {
        if (!isConstant(expr->filter())) {
            expr->filter()->accept(this);
            if (canBeFolded_) {
                expr->setFilter(fold(expr->filter()));
                if (!ok()) return;
            } else {
                canBeFolded = false;
            }
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(ReduceExpression *expr) {
    bool canBeFolded = true;
    if (!isConstant(expr->initial())) {
        expr->initial()->accept(this);
        if (canBeFolded_) {
            expr->setInitial(fold(expr->initial()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    if (!isConstant(expr->collection())) {
        expr->collection()->accept(this);
        if (canBeFolded_) {
            expr->setCollection(fold(expr->collection()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    if (!isConstant(expr->mapping())) {
        expr->mapping()->accept(this);
        if (canBeFolded_) {
            expr->setMapping(fold(expr->mapping()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    canBeFolded_ = canBeFolded;
}

void FoldConstantExprVisitor::visit(SubscriptRangeExpression *expr) {
    bool canBeFolded = true;
    if (!isConstant(expr->list())) {
        expr->list()->accept(this);
        if (canBeFolded_) {
            expr->setList(fold(expr->list()));
            if (!ok()) return;
        } else {
            canBeFolded = false;
        }
    }
    if (expr->lo() != nullptr) {
        if (!isConstant(expr->lo())) {
            expr->lo()->accept(this);
            if (canBeFolded_) {
                expr->setLo(fold(expr->lo()));
                if (!ok()) return;
            } else {
                canBeFolded = false;
            }
        }
    }
    if (expr->hi() != nullptr) {
        if (!isConstant(expr->hi())) {
            expr->hi()->accept(this);
            if (canBeFolded_) {
                expr->setHi(fold(expr->hi()));
                if (!ok()) return;
            } else {
                canBeFolded = false;
            }
        }
    }
    canBeFolded_ = canBeFolded;
}

}   // namespace graph
}   // namespace nebula
