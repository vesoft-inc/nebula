/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_DEDUCETYPEVISITOR_H_
#define GRAPH_VISITOR_DEDUCETYPEVISITOR_H_

#include <utility>  // for move

#include "common/base/Status.h"             // for Status
#include "common/datatypes/Value.h"         // for Value, Value::Type, Value...
#include "common/expression/ExprVisitor.h"  // for ExprVisitor
#include "common/thrift/ThriftTypes.h"      // for GraphSpaceID
#include "graph/context/Symbols.h"          // for ColsDef
#include "graph/context/ValidateContext.h"

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
class PropertyExpression;
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
namespace graph {
class ValidateContext;
}  // namespace graph

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
class PropertyExpression;
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

namespace graph {

class QueryContext;
class ValidateContext;

class DeduceTypeVisitor final : public ExprVisitor {
 public:
  // For testing.
  DeduceTypeVisitor(const ColsDef &inputs, GraphSpaceID space) : inputs_(inputs), space_(space) {}

  DeduceTypeVisitor(QueryContext *qctx,
                    ValidateContext *vctx,
                    const ColsDef &inputs,
                    GraphSpaceID space);
  ~DeduceTypeVisitor() = default;

  bool ok() const {
    return status_.ok();
  }

  Status status() && {
    return std::move(status_);
  }

  Value::Type type() const {
    return type_;
  }

 private:
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
  void visit(ListComprehensionExpression *) override;
  // reduce expression
  void visit(ReduceExpression *expr) override;
  // subscript range
  void visit(SubscriptRangeExpression *expr) override;

  void visitVertexPropertyExpr(PropertyExpression *expr);

  // All NULL or EMPTY will be legal in any situation.
  bool isSuperiorType(Value::Type type) {
    return type == Value::Type::NULLVALUE || type == Value::Type::__EMPTY__;
  }

  const QueryContext *qctx_{nullptr};
  const ValidateContext *vctx_{nullptr};
  const ColsDef &inputs_;
  GraphSpaceID space_;
  Status status_;
  Value::Type type_;
  Value::Type vidType_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_DEDUCETYPEVISITOR_H_
