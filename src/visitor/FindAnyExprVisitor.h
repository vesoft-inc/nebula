/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_FINDANYEXPRVISITOR_H_
#define VISITOR_FINDANYEXPRVISITOR_H_

#include <unordered_set>

#include "common/expression/Expression.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class FindAnyExprVisitor final : public ExprVisitorImpl {
public:
    explicit FindAnyExprVisitor(const std::unordered_set<Expression::Kind>& kinds);

    bool ok() const override {
        // continue if not found
        return !found_;
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
    void visit(PredicateExpression* expr) override;
    void visit(ReduceExpression* expr) override;

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
    void visit(ListComprehensionExpression* expr) override;

    void visitBinaryExpr(BinaryExpression* expr) override;

    void findExpr(const Expression* expr);

    bool found_{false};
    const Expression* expr_{nullptr};
    const std::unordered_set<Expression::Kind>& kinds_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_FINDANYEXPRVISITOR_H_
