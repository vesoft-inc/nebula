/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace nebula {
namespace graph {

template <typename T>
void FindVisitor<T>::visit(TypeCastingExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    expr->operand()->accept(this);
}

template <typename T>
void FindVisitor<T>::visit(UnaryExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    expr->operand()->accept(this);
}

template <typename T>
void FindVisitor<T>::visit(FunctionCallExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    for (const auto& arg : expr->args()->args()) {
        arg->accept(this);
        if (!needFindAll_ && found_) return;
    }
}

template <typename T>
void FindVisitor<T>::visit(AggregateExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    expr->arg()->accept(this);
}

template <typename T>
void FindVisitor<T>::visit(ListExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    for (const auto& item : expr->items()) {
        item->accept(this);
        if (!needFindAll_ && found_) return;
    }
}

template <typename T>
void FindVisitor<T>::visit(SetExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    for (const auto& item : expr->items()) {
        item->accept(this);
        if (!needFindAll_ && found_) return;
    }
}

template <typename T>
void FindVisitor<T>::visit(MapExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    for (const auto& pair : expr->items()) {
        pair.second->accept(this);
        if (!needFindAll_ && found_) return;
    }
}

template <typename T>
void FindVisitor<T>::visit(CaseExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;

    if (expr->hasCondition()) {
        expr->condition()->accept(this);
        if (!needFindAll_ && found_) return;
    }
    if (expr->hasDefault()) {
        expr->defaultResult()->accept(this);
        if (!needFindAll_ && found_) return;
    }
    for (const auto& whenThen : expr->cases()) {
        whenThen.when->accept(this);
        if (!needFindAll_ && found_) return;
        whenThen.then->accept(this);
        if (!needFindAll_ && found_) return;
    }
}

template <typename T>
void FindVisitor<T>::visit(PredicateExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;

    expr->collection()->accept(this);
    if (!needFindAll_ && found_) return;
    if (expr->hasFilter()) {
        expr->filter()->accept(this);
    }
}

template <typename T>
void FindVisitor<T>::visit(ReduceExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;

    expr->initial()->accept(this);
    if (!needFindAll_ && found_) return;
    expr->collection()->accept(this);
    if (!needFindAll_ && found_) return;
    expr->mapping()->accept(this);
    if (!needFindAll_ && found_) return;
}

template <typename T>
void FindVisitor<T>::visit(ListComprehensionExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;

    expr->collection()->accept(this);
    if (!needFindAll_ && found_) return;

    if (expr->hasFilter()) {
        expr->filter()->accept(this);
        if (!needFindAll_ && found_) return;
    }

    if (expr->hasMapping()) {
        expr->mapping()->accept(this);
    }
}

template <typename T>
void FindVisitor<T>::visit(ConstantExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(EdgePropertyExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(TagPropertyExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(InputPropertyExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(VariablePropertyExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(SourcePropertyExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(DestPropertyExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(EdgeSrcIdExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(EdgeTypeExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(EdgeRankExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(EdgeDstIdExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(UUIDExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(VariableExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(VersionedVariableExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(LabelExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(LabelAttributeExpression *expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(VertexExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(EdgeExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visit(ColumnExpression* expr) {
    findInCurrentExpr(expr);
}

template <typename T>
void FindVisitor<T>::visitBinaryExpr(BinaryExpression* expr) {
    findInCurrentExpr(expr);
    if (!needFindAll_ && found_) return;
    expr->left()->accept(this);
    if (!needFindAll_ && found_) return;
    expr->right()->accept(this);
}

template <typename T>
void FindVisitor<T>::findInCurrentExpr(Expression* expr) {
    if (find(expr)) {
        found_ = true;
        foundExprs_.emplace_back(expr);
    }
}

}   // namespace graph
}   // namespace nebula
