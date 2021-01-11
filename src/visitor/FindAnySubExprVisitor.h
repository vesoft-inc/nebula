/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_FINDANYSUBEXPRVISITOR_H_
#define VISITOR_FINDANYSUBEXPRVISITOR_H_

#include <unordered_set>

#include "common/expression/Expression.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class FindAnySubExprVisitor final : public ExprVisitorImpl {
public:
    explicit FindAnySubExprVisitor(std::unordered_set<Expression*> &subExprs,
                                   bool needRecursiveSearch);

    bool ok() const override {
        return !found_;
    }

    bool found() const {
        return found_;
    }

    const Expression* expr() const {
        return expr_;
    }

private:
    using ExprVisitorImpl::visit;

    void visit(TypeCastingExpression* expr) override;
    void visit(UnaryExpression* expr) override;
    void visit(FunctionCallExpression* expr) override;
    void visit(AggregateExpression* expr) override;
    void visit(ListExpression* expr) override;
    void visit(SetExpression* expr) override;
    void visit(MapExpression* expr) override;
    void visit(CaseExpression* expr) override;

    void visit(ConstantExpression* expr) override;
    void visit(EdgePropertyExpression* expr) override;
    void visit(TagPropertyExpression* expr) override;
    void visit(InputPropertyExpression* expr) override;
    void visit(VariablePropertyExpression* expr) override;
    void visit(SourcePropertyExpression* expr) override;
    void visit(DestPropertyExpression* expr) override;
    void visit(EdgeSrcIdExpression* expr) override;
    void visit(EdgeTypeExpression* expr) override;
    void visit(EdgeRankExpression* expr) override;
    void visit(EdgeDstIdExpression* expr) override;
    void visit(UUIDExpression* expr) override;
    void visit(VariableExpression* expr) override;
    void visit(VersionedVariableExpression* expr) override;
    void visit(LabelExpression* expr) override;
    void visit(VertexExpression* expr) override;
    void visit(EdgeExpression* expr) override;
    void visit(ColumnExpression* expr) override;
    void visitBinaryExpr(BinaryExpression* expr) override;

    void checkExprKind(const Expression*, const Expression*);

    template <typename T>
    void compareWithSubExprs(T* expr);

    bool found_{false};
    // need continue search
    bool continue_{true};
    const Expression* expr_{nullptr};
    const std::unordered_set<Expression*> subExprs_;
    bool needRecursiveSearch_{false};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_FINDANYSUBEXPRVISITOR_H_
