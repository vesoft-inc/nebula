/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITEINPUTPROPVISITOR_H_
#define VISITOR_REWRITEINPUTPROPVISITOR_H_

#include "common/expression/ExprVisitor.h"
#include "parser/Clauses.h"

namespace nebula {
namespace graph {

class RewriteInputPropVisitor final : public ExprVisitor {
public:
    explicit RewriteInputPropVisitor(
        const std::unordered_map<std::string, YieldColumn *> &propExprColMap)
        : propExprColMap_(propExprColMap) {}
    ~RewriteInputPropVisitor() = default;

    bool ok() const {
        return result_ != nullptr;
    }

    const Status& status() const {
        return status_;
    }

    std::unique_ptr<Expression> result() && {
        return std::move(result_);
    }

private:
    void visit(ConstantExpression *) override;
    void visit(UnaryExpression *) override;
    void visit(TypeCastingExpression *) override;
    void visit(LabelExpression *) override;
    void visit(LabelAttributeExpression *) override;
    // binary expression
    void visit(ArithmeticExpression *) override;
    void visit(RelationalExpression *) override;
    void visit(SubscriptExpression *) override;
    void visit(AttributeExpression *) override;
    void visit(LogicalExpression *) override;
    // function call
    void visit(FunctionCallExpression *) override;
    void visit(UUIDExpression *) override;
    // variable expression
    void visit(VariableExpression *) override;
    void visit(VersionedVariableExpression *) override;
    // container expression
    void visit(ListExpression *) override;
    void visit(SetExpression *) override;
    void visit(MapExpression *) override;
    // property Expression
    void visit(TagPropertyExpression *) override;
    void visit(EdgePropertyExpression *) override;
    void visit(InputPropertyExpression *) override;
    void visit(VariablePropertyExpression *) override;
    void visit(DestPropertyExpression *) override;
    void visit(SourcePropertyExpression *) override;
    void visit(EdgeSrcIdExpression *) override;
    void visit(EdgeTypeExpression *) override;
    void visit(EdgeRankExpression *) override;
    void visit(EdgeDstIdExpression *) override;
    // vertex/edge expression
    void visit(VertexExpression *) override;
    void visit(EdgeExpression *) override;
    // case expression
    void visit(CaseExpression *) override;
    // path build expression
    void visit(PathBuildExpression *expr) override;

    void visitBinaryExpr(BinaryExpression *expr);
    void visitUnaryExpr(UnaryExpression *expr);
    void visitVertexEdgePropExpr(PropertyExpression *expr);
    void reportError(const Expression *);

private:
    const std::unordered_map<std::string, YieldColumn *> &propExprColMap_;

    std::unique_ptr<Expression> result_;
    Status status_;
};

}  //  namespace graph
}  //  namespace nebula

#endif   // VISITOR_REWRITEINPUTPROPVISITOR_H_
