/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_VISITOR_EXPRVISITORIMPL_H_
#define COMMON_VISITOR_EXPRVISITORIMPL_H_

#include "common/expression/ColumnExpression.h"
#include "common/expression/ExprVisitor.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/VertexExpression.h"

namespace nebula {

class ExprVisitorImpl : public ExprVisitor {
 public:
  // leaf expression nodes
  void visit(ConstantExpression *) override {}
  void visit(LabelExpression *) override{};
  void visit(UUIDExpression *) override{};
  void visit(VariableExpression *) override{};
  void visit(VersionedVariableExpression *) override{};
  void visit(InputPropertyExpression *) override{};
  void visit(VariablePropertyExpression *) override{};
  void visit(DestPropertyExpression *) override{};
  void visit(SourcePropertyExpression *) override{};
  void visit(VertexExpression *) override{};
  void visit(EdgeExpression *) override{};
  void visit(ColumnExpression *) override{};
  // unary expression
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
  // match path pattern expression
  void visit(MatchPathPatternExpression *expr) override;

 protected:
  using ExprVisitor::visit;

  virtual void visitBinaryExpr(BinaryExpression *expr);
  virtual bool ok() const {
    return true;
  }
};

}  // namespace nebula

#endif  // COMMON_VISITOR_EXPRVISITORIMPL_H_
