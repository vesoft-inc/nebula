/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_EXTRACTGROUPSUITEVISITOR_H_
#define GRAPH_VISITOR_EXTRACTGROUPSUITEVISITOR_H_

#include "common/expression/ExprVisitorImpl.h"
#include "graph/context/ast/CypherAstContext.h"

namespace nebula {
namespace graph {

class ExtractGroupSuiteVisitor : public ExprVisitorImpl {
 public:
  explicit ExtractGroupSuiteVisitor(QueryContext *qctx = nullptr) : qctx_(qctx) {}

  bool ok() const override {
    return true;
  }

  GroupSuite groupSuite() {
    return groupSuite_;
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
  // match path pattern expression
  void visit(MatchPathPatternExpression *expr) override;

 private:
  template <typename T>
  void internalVisit(T *expr);

  template <typename T>
  void pushGroupSuite(T *expr);

  GroupSuite groupSuite_;
  QueryContext *qctx_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_EXTRACTGROUPSUITEVISITOR_H_
