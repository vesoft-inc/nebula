/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXPRVISITORBASE_H
#define STORAGE_EXPRVISITORBASE_H

#include "common/base/Logging.h"            // for LOG, LogMessageFatal, _LO...
#include "common/expression/ExprVisitor.h"  // for ExprVisitor
#include "common/expression/Expression.h"   // for Expression

namespace nebula {
class AggregateExpression;
class ArithmeticExpression;
class AttributeExpression;
class CaseExpression;
class ColumnExpression;
class ConstantExpression;
class DestPropertyExpression;
class EdgeDstIdExpression;
class EdgeExpression;
class EdgePropertyExpression;
class EdgeRankExpression;
class EdgeSrcIdExpression;
class EdgeTypeExpression;
class FunctionCallExpression;
class InputPropertyExpression;
class LabelAttributeExpression;
class LabelExpression;
class LabelTagPropertyExpression;
class ListComprehensionExpression;
class ListExpression;
class LogicalExpression;
class MapExpression;
class PathBuildExpression;
class PredicateExpression;
class ReduceExpression;
class RelationalExpression;
class SetExpression;
class SourcePropertyExpression;
class SubscriptExpression;
class SubscriptRangeExpression;
class TagPropertyExpression;
class TypeCastingExpression;
class UUIDExpression;
class UnaryExpression;
class VariableExpression;
class VariablePropertyExpression;
class VersionedVariableExpression;
class VertexExpression;

class AggregateExpression;
class ArithmeticExpression;
class AttributeExpression;
class CaseExpression;
class ColumnExpression;
class ConstantExpression;
class DestPropertyExpression;
class EdgeDstIdExpression;
class EdgeExpression;
class EdgePropertyExpression;
class EdgeRankExpression;
class EdgeSrcIdExpression;
class EdgeTypeExpression;
class FunctionCallExpression;
class InputPropertyExpression;
class LabelAttributeExpression;
class LabelExpression;
class LabelTagPropertyExpression;
class ListComprehensionExpression;
class ListExpression;
class LogicalExpression;
class MapExpression;
class PathBuildExpression;
class PredicateExpression;
class ReduceExpression;
class RelationalExpression;
class SetExpression;
class SourcePropertyExpression;
class SubscriptExpression;
class SubscriptRangeExpression;
class TagPropertyExpression;
class TypeCastingExpression;
class UUIDExpression;
class UnaryExpression;
class VariableExpression;
class VariablePropertyExpression;
class VersionedVariableExpression;
class VertexExpression;

namespace storage {

class ExprVisitorBase : public ::nebula::ExprVisitor {
 public:
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
  void visit(LabelTagPropertyExpression *expr) override;
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
  using ::nebula::ExprVisitor::visit;
  inline void fatal(Expression *expr) {
    LOG(FATAL) << "Unexpect expression kind " << static_cast<int>(expr->kind()) << " at storage";
  }
};
}  // namespace storage
}  // namespace nebula
#endif
