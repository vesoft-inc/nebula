/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_REWRITESYMEXPRVISITOR_H_
#define VISITOR_REWRITESYMEXPRVISITOR_H_

#include <memory>

#include "common/expression/ExprVisitor.h"

namespace nebula {

class Expression;

namespace graph {

class RewriteSymExprVisitor final : public ExprVisitor {
public:
    RewriteSymExprVisitor(ObjectPool *objPool, const std::string &sym, bool isEdge);

    bool hasWrongType() const {
        return hasWrongType_;
    }

    Expression* expr() {
        return expr_;
    }

    void visit(ConstantExpression *expr) override;
    void visit(UnaryExpression *expr) override;
    void visit(TypeCastingExpression *expr) override;
    void visit(LabelExpression *expr) override;
    void visit(LabelAttributeExpression *expr) override;
    // binary expression
    void visit(ArithmeticExpression *expr) override;
    void visit(RelationalExpression *expr) override;
    void visit(SubscriptExpression *expr) override;
    void visit(AttributeExpression *expr) override;
    void visit(LogicalExpression *expr) override;
    // function call
    void visit(FunctionCallExpression *expr) override;
    void visit(AggregateExpression *expr) override;
    void visit(UUIDExpression *expr) override;
    // variable expression
    void visit(VariableExpression *expr) override;
    void visit(VersionedVariableExpression *expr) override;
    // container expression
    void visit(ListExpression *expr) override;
    void visit(SetExpression *expr) override;
    void visit(MapExpression *expr) override;
    // property Expression
    void visit(TagPropertyExpression *expr) override;
    void visit(EdgePropertyExpression *expr) override;
    void visit(InputPropertyExpression *expr) override;
    void visit(VariablePropertyExpression *expr) override;
    void visit(DestPropertyExpression *expr) override;
    void visit(SourcePropertyExpression *expr) override;
    void visit(EdgeSrcIdExpression *expr) override;
    void visit(EdgeTypeExpression *expr) override;
    void visit(EdgeRankExpression *expr) override;
    void visit(EdgeDstIdExpression *expr) override;
    // vertex/edge expression
    void visit(VertexExpression *expr) override;
    void visit(EdgeExpression *expr) override;
    // case expression
    void visit(CaseExpression *expr) override;
    // path build expression
    void visit(PathBuildExpression *expr) override;
    // column expression
    void visit(ColumnExpression *expr) override;
    // predicate expression
    void visit(PredicateExpression *expr) override;
    // list comprehension expression
    void visit(ListComprehensionExpression *expr) override;
    // reduce expression
    void visit(ReduceExpression *expr) override;
    // subscript range expression
    void visit(SubscriptRangeExpression *expr) override;

private:
    void visitBinaryExpr(BinaryExpression *expr);

    ObjectPool *pool_;
    const std::string &sym_;
    bool hasWrongType_{false};
    bool isEdge_{false};
    Expression *expr_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_REWRITESYMEXPRVISITOR_H_
