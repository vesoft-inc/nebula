/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/FindAnySubExprVisitor.h"

namespace nebula {
namespace graph {

FindAnySubExprVisitor::FindAnySubExprVisitor(std::unordered_set<Expression*> &subExprs,
                                             bool needRecursiveSearch)
    : subExprs_(subExprs), needRecursiveSearch_(needRecursiveSearch) {
    DCHECK(!subExprs_.empty());
}

void FindAnySubExprVisitor::visit(TypeCastingExpression *expr) {
    compareWithSubExprs<TypeCastingExpression>(expr);
    if (needRecursiveSearch_) {
        expr->operand()->accept(this);
    }
}

void FindAnySubExprVisitor::visit(UnaryExpression *expr) {
    compareWithSubExprs<UnaryExpression>(expr);
    if (needRecursiveSearch_) {
        expr->operand()->accept(this);
    }
}

void FindAnySubExprVisitor::visit(FunctionCallExpression *expr) {
    compareWithSubExprs<FunctionCallExpression>(expr);
    if (needRecursiveSearch_) {
        for (const auto &arg : expr->args()->args()) {
            arg->accept(this);
            if (found_) return;
        }
    }
}

void FindAnySubExprVisitor::visit(AggregateExpression *expr) {
    compareWithSubExprs<AggregateExpression>(expr);
    if (needRecursiveSearch_) {
        expr->arg()->accept(this);
    }
}

void FindAnySubExprVisitor::visit(ListExpression *expr) {
    compareWithSubExprs<ListExpression>(expr);
    if (needRecursiveSearch_) {
        for (const auto &item : expr->items()) {
            item->accept(this);
            if (found_) return;
        }
    }
}

void FindAnySubExprVisitor::visit(SetExpression *expr) {
    compareWithSubExprs<SetExpression>(expr);
    if (needRecursiveSearch_) {
        for (const auto &item : expr->items()) {
            item->accept(this);
            if (found_) return;
        }
    }
}

void FindAnySubExprVisitor::visit(MapExpression *expr) {
    compareWithSubExprs<MapExpression>(expr);
    if (needRecursiveSearch_) {
        for (const auto &pair : expr->items()) {
            pair.second->accept(this);
            if (found_) return;
        }
    }
}

void FindAnySubExprVisitor::visit(CaseExpression *expr) {
    compareWithSubExprs<CaseExpression>(expr);
    if (needRecursiveSearch_) {
        if (expr->hasCondition()) {
            expr->condition()->accept(this);
            if (found_) return;
        }
        if (expr->hasDefault()) {
            expr->defaultResult()->accept(this);
            if (found_) return;
        }
        for (const auto &whenThen : expr->cases()) {
            whenThen.when->accept(this);
            if (found_) return;
            whenThen.then->accept(this);
            if (found_) return;
        }
    }
}

void FindAnySubExprVisitor::visit(ConstantExpression *expr) {
    compareWithSubExprs<ConstantExpression>(expr);
}

void FindAnySubExprVisitor::visit(EdgePropertyExpression *expr) {
    compareWithSubExprs<EdgePropertyExpression>(expr);
}

void FindAnySubExprVisitor::visit(TagPropertyExpression *expr) {
    compareWithSubExprs<TagPropertyExpression>(expr);
}

void FindAnySubExprVisitor::visit(InputPropertyExpression *expr) {
    compareWithSubExprs<InputPropertyExpression>(expr);
}

void FindAnySubExprVisitor::visit(VariablePropertyExpression *expr) {
    compareWithSubExprs<VariablePropertyExpression>(expr);
}

void FindAnySubExprVisitor::visit(SourcePropertyExpression *expr) {
    compareWithSubExprs<SourcePropertyExpression>(expr);
}

void FindAnySubExprVisitor::visit(DestPropertyExpression *expr) {
    compareWithSubExprs<DestPropertyExpression>(expr);
}

void FindAnySubExprVisitor::visit(EdgeSrcIdExpression *expr) {
    compareWithSubExprs<EdgeSrcIdExpression>(expr);
}

void FindAnySubExprVisitor::visit(EdgeTypeExpression *expr) {
    compareWithSubExprs<EdgeTypeExpression>(expr);
}

void FindAnySubExprVisitor::visit(EdgeRankExpression *expr) {
    compareWithSubExprs<EdgeRankExpression>(expr);
}

void FindAnySubExprVisitor::visit(EdgeDstIdExpression *expr) {
    compareWithSubExprs<EdgeDstIdExpression>(expr);
}

void FindAnySubExprVisitor::visit(UUIDExpression *expr) {
    compareWithSubExprs<UUIDExpression>(expr);
}

void FindAnySubExprVisitor::visit(VariableExpression *expr) {
    compareWithSubExprs<VariableExpression>(expr);
}

void FindAnySubExprVisitor::visit(VersionedVariableExpression *expr) {
    compareWithSubExprs<VersionedVariableExpression>(expr);
}

void FindAnySubExprVisitor::visit(LabelExpression *expr) {
    compareWithSubExprs<LabelExpression>(expr);
}

void FindAnySubExprVisitor::visit(VertexExpression *expr) {
    compareWithSubExprs<VertexExpression>(expr);
}

void FindAnySubExprVisitor::visit(EdgeExpression *expr) {
    compareWithSubExprs<EdgeExpression>(expr);
}

void FindAnySubExprVisitor::visit(ColumnExpression *expr) {
    compareWithSubExprs<ColumnExpression>(expr);
}

void FindAnySubExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
    compareWithSubExprs<BinaryExpression>(expr);
    if (needRecursiveSearch_) {
        expr->left()->accept(this);
        if (found_) return;
        expr->right()->accept(this);
    }
}

void FindAnySubExprVisitor::checkExprKind(const Expression *expr, const Expression *sub_expr) {
    if (expr == sub_expr) {
        found_ = true;
        continue_ = false;
    } else if (expr == nullptr || sub_expr == nullptr) {
        continue_ = false;
    } else if (expr->kind() != sub_expr->kind()) {
       continue_ = false;
    }
}

template <typename T>
void FindAnySubExprVisitor::compareWithSubExprs(T* expr) {
    for (Expression* sub : subExprs_) {
        continue_ = true;
        checkExprKind(expr, sub);
        if (found_) return;
        if (!continue_) continue;
        if (*static_cast<T*>(sub) == *expr) {
            found_ = true;
            return;
        }
    }
}

}   // namespace graph
}   // namespace nebula
