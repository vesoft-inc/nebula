/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_CHECKDEPTHVISITOR_H_
#define GRAPH_VISITOR_CHECKDEPTHVISITOR_H_

#include "common/base/Status.h"
#include "common/datatypes/Value.h"
#include "common/expression/ExprVisitor.h"
#include "graph/context/ValidateContext.h"

namespace nebula {
namespace graph {

class QueryContext;

class CheckDepthVisitor final : public ExprVisitor {
 public:
  CheckDepthVisitor();
  ~CheckDepthVisitor() = default;

  bool ok() const { return status_.ok(); }

  Status status() && { return std::move(status_); }

 private:
  void visit(ConstantExpression *) override;
  void visit(UnaryExpression *expr) override;
  void visit(TypeCastingExpression *expr) override;
  void visit(LabelExpression *) override;
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
  void visit(CaseExpression *expr) override;
  // path build expression
  void visit(PathBuildExpression *) override;
  // column expression
  void visit(ColumnExpression *) override;
  // predicate expression
  void visit(PredicateExpression *expr) override;
  // list comprehension expression
  void visit(ListComprehensionExpression *expr) override;
  // reduce expression
  void visit(ReduceExpression *expr) override;
  // subscript range
  void visit(SubscriptRangeExpression *expr) override;

  inline void checkDepth() {
    if (++depth > MAX_DEPTH) {
      status_ = Status::SemanticError(
          "The above expression is not a valid expression, "
          "because its depth exceeds the maximum depth");
    }
  }

  inline void recoverDepth() { --depth; }

  Status status_;

  int32_t depth = 0;
  const int32_t MAX_DEPTH = 512;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_CHECKDEPTHVISITOR_H_
