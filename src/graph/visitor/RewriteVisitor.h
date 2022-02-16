/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_REWRITEVISITOR_H_
#define GRAPH_VISITOR_REWRITEVISITOR_H_

#include <functional>     // for function
#include <unordered_set>  // for unordered_set
#include <utility>        // for move

#include "common/expression/Expression.h"   // for Expression::Kind, Expression
#include "graph/visitor/ExprVisitorImpl.h"  // for ExprVisitorImpl

namespace nebula {
class AggregateExpression;
class ArithmeticExpression;
class AttributeExpression;
class BinaryExpression;
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
class BinaryExpression;
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

namespace graph {

class RewriteVisitor final : public ExprVisitorImpl {
 public:
  using Matcher = std::function<bool(const Expression*)>;
  using Rewriter = std::function<Expression*(const Expression*)>;

  static Expression* transform(const Expression* expr, Matcher matcher, Rewriter rewriter);

  static Expression* transform(const Expression* expr,
                               Matcher matcher,
                               Rewriter rewriter,
                               const std::unordered_set<Expression::Kind>& needVisitedTypes);

  const Matcher matcher() const {
    return matcher_;
  }

  const Rewriter rewriter() const {
    return rewriter_;
  }

 private:
  RewriteVisitor(Matcher matcher, Rewriter rewriter)
      : matcher_(std::move(matcher)), rewriter_(std::move(rewriter)) {}

  RewriteVisitor(Matcher matcher,
                 Rewriter rewriter,
                 const std::unordered_set<Expression::Kind>& needVisitedTypes)
      : matcher_(std::move(matcher)),
        rewriter_(std::move(rewriter)),
        needVisitedTypes_(std::move(needVisitedTypes)) {}

  bool ok() const override {
    return true;
  }

 private:
  using ExprVisitorImpl::visit;
  void visit(TypeCastingExpression*) override;
  void visit(UnaryExpression*) override;
  void visit(FunctionCallExpression*) override;
  void visit(AggregateExpression*) override;
  void visit(ListExpression*) override;
  void visit(SetExpression*) override;
  void visit(MapExpression*) override;
  void visit(CaseExpression*) override;
  void visit(ReduceExpression*) override;
  void visit(PredicateExpression*) override;
  void visit(ListComprehensionExpression*) override;
  void visit(LogicalExpression*) override;
  void visit(ArithmeticExpression*) override;
  void visit(AttributeExpression*) override;
  void visit(RelationalExpression*) override;
  void visit(SubscriptExpression*) override;
  void visit(PathBuildExpression*) override;
  void visit(LabelTagPropertyExpression*) override;
  void visit(SubscriptRangeExpression*) override;
  void visit(ConstantExpression*) override {}
  void visit(LabelExpression*) override {}
  void visit(UUIDExpression*) override {}
  void visit(LabelAttributeExpression*) override {}
  void visit(VariableExpression*) override {}
  void visit(VersionedVariableExpression*) override {}
  void visit(TagPropertyExpression*) override {}
  void visit(EdgePropertyExpression*) override {}
  void visit(InputPropertyExpression*) override {}
  void visit(VariablePropertyExpression*) override {}
  void visit(DestPropertyExpression*) override {}
  void visit(SourcePropertyExpression*) override {}
  void visit(EdgeSrcIdExpression*) override {}
  void visit(EdgeTypeExpression*) override {}
  void visit(EdgeRankExpression*) override {}
  void visit(EdgeDstIdExpression*) override {}
  void visit(VertexExpression*) override {}
  void visit(EdgeExpression*) override {}
  void visit(ColumnExpression*) override {}

  void visitBinaryExpr(BinaryExpression*) override;
  bool care(Expression::Kind kind);

 private:
  Matcher matcher_;
  Rewriter rewriter_;

  std::unordered_set<Expression::Kind> needVisitedTypes_{};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_REWRITEVISITOR_H_
