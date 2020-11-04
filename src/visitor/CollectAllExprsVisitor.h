/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_COLLECTALLEXPRSVISITOR_H_
#define VISITOR_COLLECTALLEXPRSVISITOR_H_

#include <unordered_set>

#include "common/expression/Expression.h"
#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class CollectAllExprsVisitor final : public ExprVisitorImpl {
public:
    explicit CollectAllExprsVisitor(const std::unordered_set<Expression::Kind>& exprKinds);
    bool ok() const override {
        return !exprKinds_.empty();
    }

    std::vector<const Expression*> exprs() && {
        return std::move(exprs_);
    }

private:
    using ExprVisitorImpl::visit;

    void visit(TypeCastingExpression* expr) override;
    void visit(UnaryExpression* expr) override;
    void visit(FunctionCallExpression* expr) override;
    void visit(ListExpression* expr) override;
    void visit(SetExpression* expr) override;
    void visit(MapExpression* expr) override;

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
    void visit(LabelAttributeExpression* expr) override;
    void visit(AttributeExpression* expr) override;
    void visit(VertexExpression* expr) override;
    void visit(EdgeExpression* expr) override;
    // TODO : CaseExpression
    void visit(CaseExpression *) override {};

    void visitBinaryExpr(BinaryExpression* expr) override;
    void collectExpr(const Expression* expr);

    const std::unordered_set<Expression::Kind>& exprKinds_;
    std::vector<const Expression*> exprs_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_COLLECTALLEXPRSVISITOR_H_
