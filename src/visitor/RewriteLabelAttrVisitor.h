/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITELABELATTRVISITOR_H_
#define VISITOR_REWRITELABELATTRVISITOR_H_

#include <memory>
#include <vector>

#include "visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class RewriteLabelAttrVisitor final : public ExprVisitorImpl {
public:
    explicit RewriteLabelAttrVisitor(bool isTag);

    bool ok() const override {
        return true;
    }

private:
    using ExprVisitorImpl::visit;

    void visit(TypeCastingExpression *expr) override;
    void visit(UnaryExpression *expr) override;
    void visit(FunctionCallExpression *expr) override;
    void visit(ListExpression *expr) override;
    void visit(SetExpression *expr) override;
    void visit(MapExpression *expr) override;
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

    void visitBinaryExpr(BinaryExpression *expr) override;

    Expression *createExpr(const LabelAttributeExpression *expr);
    std::vector<std::unique_ptr<Expression>> rewriteExprList(
        const std::vector<std::unique_ptr<Expression>> &exprs);
    static bool isLabelAttrExpr(const Expression *expr);

    bool isTag_{false};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_REWRITELABELATTRVISITOR_H_
