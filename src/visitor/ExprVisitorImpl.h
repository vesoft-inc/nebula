/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VISITOR_EXPRVISITORIMPL_H_
#define VISITOR_EXPRVISITORIMPL_H_

#include "common/expression/ExprVisitor.h"

namespace nebula {
namespace graph {

class ExprVisitorImpl : public ExprVisitor {
public:
    void visit(UnaryExpression *expr) override;
    void visit(TypeCastingExpression *expr) override;
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
    // container expression
    void visit(ListExpression *expr) override;
    void visit(SetExpression *expr) override;
    void visit(MapExpression *expr) override;
    // case expression
    void visit(CaseExpression *expr) override;
    // path build expression
    void visit(PathBuildExpression *expr) override;
    // predicate expression
    void visit(PredicateExpression *expr) override;
    // list comprehension expression
    void visit(ListComprehensionExpression *expr) override;
    // reduce expression
    void visit(ReduceExpression *expr) override;
    // subscript range expression
    void visit(SubscriptRangeExpression *expr) override;

protected:
    using ExprVisitor::visit;

    virtual void visitBinaryExpr(BinaryExpression *expr);
    virtual bool ok() const = 0;
};

}   // namespace graph
}   // namespace nebula

#endif   // VISITOR_EXPRVISITORIMPL_H_
