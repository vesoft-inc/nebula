/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITEAGGEXPRVISITOR_H_
#define VISITOR_REWRITEAGGEXPRVISITOR_H_

#include <memory>
#include <vector>

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class RewriteAggExprVisitor final : public ExprVisitorImpl {
public:
    explicit RewriteAggExprVisitor(std::string* var,
                                   std::string* prop);

    bool ok() const override {
        return true;
    }

private:
    using ExprVisitorImpl::visit;

    void visit(TypeCastingExpression *expr) override;
    void visit(FunctionCallExpression *expr) override;
    void visit(ArithmeticExpression *expr) override;

    void visit(UnaryExpression *) override {}
    void visit(ListExpression *) override {}
    void visit(SetExpression *) override {}
    void visit(MapExpression *) override {}
    void visit(CaseExpression *) override {}
    void visit(ConstantExpression *) override {}
    void visit(LabelExpression *) override {}
    void visit(UUIDExpression *) override {}
    void visit(LabelAttributeExpression *) override {}
    void visit(VariableExpression *) override {}
    void visit(VersionedVariableExpression *) override {}
    void visit(TagPropertyExpression *) override {}
    void visit(EdgePropertyExpression *) override {}
    void visit(InputPropertyExpression *) override {}
    void visit(VariablePropertyExpression *) override {}
    void visit(DestPropertyExpression *) override {}
    void visit(SourcePropertyExpression *) override {}
    void visit(EdgeSrcIdExpression *) override {}
    void visit(EdgeTypeExpression *) override {}
    void visit(EdgeRankExpression *) override {}
    void visit(EdgeDstIdExpression *) override {}
    void visit(VertexExpression *) override {}
    void visit(EdgeExpression *) override {}
    void visit(ColumnExpression *) override {}
    void visitBinaryExpr(BinaryExpression *) override;

    static bool isAggExpr(const Expression* expr);

private:
    std::unique_ptr<std::string> var_;
    std::unique_ptr<std::string> prop_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_REWRITEAGGEXPRVISITOR_H_
