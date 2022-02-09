/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef GRAPH_VISITOR_EXTRACTPROPEXPRVISITOR_H_
#define GRAPH_VISITOR_EXTRACTPROPEXPRVISITOR_H_

#include "graph/context/ValidateContext.h"
#include "graph/visitor/ExprVisitorImpl.h"
#include "parser/Clauses.h"

namespace nebula {
namespace graph {

class ValidateContext;

class ExtractPropExprVisitor final : public ExprVisitorImpl {
 public:
  ExtractPropExprVisitor(ValidateContext *vctx,
                         YieldColumns *srcAndEdgePropCols,
                         YieldColumns *dstPropCols,
                         YieldColumns *inputPropCols,
                         std::unordered_map<std::string, YieldColumn *> &propExprColMap,
                         std::unordered_set<std::string> &uniqueEdgeVertexCol);

  ~ExtractPropExprVisitor() = default;

  bool ok() const override {
    return status_.ok();
  }

  const Status &status() const {
    return status_;
  }

 private:
  using ExprVisitorImpl::visit;

  void visit(ConstantExpression *) override;
  void visit(LabelExpression *) override;
  void visit(UUIDExpression *) override;
  void visit(UnaryExpression *) override;
  void visit(LabelAttributeExpression *) override;
  // variable expression
  void visit(VariableExpression *) override;
  void visit(VersionedVariableExpression *) override;
  // property Expression
  void visit(LabelTagPropertyExpression *) override;
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
  // binary expression
  void visit(SubscriptExpression *) override;
  // column expression
  void visit(ColumnExpression *) override;

  void visitVertexEdgePropExpr(PropertyExpression *);
  void visitPropertyExpr(PropertyExpression *);
  void reportError(const Expression *);

 private:
  ValidateContext *vctx_{nullptr};
  YieldColumns *srcAndEdgePropCols_{nullptr};
  YieldColumns *dstPropCols_{nullptr};
  YieldColumns *inputPropCols_{nullptr};
  std::unordered_map<std::string, YieldColumn *> &propExprColMap_;
  std::unordered_set<std::string> &uniqueEdgeVertexCol_;

  Status status_;
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_EXTRACTPROPEXPRVISITOR_H_
