/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_EVALUABLEEXPRVISITOR_H_
#define GRAPH_VISITOR_EVALUABLEEXPRVISITOR_H_

#include "common/expression/ExprVisitorImpl.h"

namespace nebula {
namespace graph {
class QueryContext;

class EvaluableExprVisitor : public ExprVisitorImpl {
 public:
  explicit EvaluableExprVisitor(const QueryContext *qctx = nullptr);
  bool ok() const override {
    return isEvaluable_;
  }

 private:
  using ExprVisitorImpl::visit;

  void visit(ConstantExpression *) override;

  void visit(LabelExpression *) override;

  void visit(UUIDExpression *) override;

  void visit(VariableExpression *) override;

  void visit(VersionedVariableExpression *) override;

  void visit(TagPropertyExpression *) override;

  void visit(EdgePropertyExpression *) override;

  void visit(LabelTagPropertyExpression *) override;

  void visit(InputPropertyExpression *) override;

  void visit(VariablePropertyExpression *) override;

  void visit(DestPropertyExpression *) override;

  void visit(SourcePropertyExpression *) override;

  void visit(EdgeSrcIdExpression *) override;

  void visit(EdgeTypeExpression *) override;

  void visit(EdgeRankExpression *) override;

  void visit(EdgeDstIdExpression *) override;

  void visit(VertexExpression *) override;

  void visit(EdgeExpression *) override;

  void visit(ColumnExpression *) override;

  void visit(SubscriptRangeExpression *) override;

  void visitBinaryExpr(BinaryExpression *) override;

  bool isEvaluable_{true};
  const QueryContext *qctx_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_EVALUABLEEXPRVISITOR_H_
