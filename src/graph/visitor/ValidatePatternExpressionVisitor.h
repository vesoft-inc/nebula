// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include "common/expression/ExprVisitorImpl.h"
#include "common/expression/Expression.h"

namespace nebula {
namespace graph {

class ValidateContext;

class ValidatePatternExpressionVisitor final : public ExprVisitorImpl {
 public:
  explicit ValidatePatternExpressionVisitor(ObjectPool *pool, ValidateContext *vctx)
      : pool_(pool), vctx_(vctx) {}

  bool ok() const override {
    // TODO: delete this interface
    return true;
  }

 private:
  using ExprVisitorImpl::visit;
  void visit(ConstantExpression *) override {}
  void visit(LabelExpression *) override {}
  void visit(UUIDExpression *) override {}
  void visit(VariableExpression *) override {}
  void visit(VersionedVariableExpression *) override {}
  void visit(TagPropertyExpression *) override {}
  void visit(LabelTagPropertyExpression *) override {}
  void visit(EdgePropertyExpression *) override {}
  void visit(InputPropertyExpression *) override {}
  void visit(VariablePropertyExpression *) override {}
  void visit(DestPropertyExpression *) override {}
  void visit(SourcePropertyExpression *) override {}
  void visit(EdgeSrcIdExpression *) override {}
  void visit(EdgeTypeExpression *) override {}
  void visit(EdgeRankExpression *) override {}
  void visit(EdgeDstIdExpression *) override {}
  void visit(VertexExpression *) override {}
  void visit(EdgeExpression *) override {}
  void visit(ColumnExpression *) override {}

  void visit(ListComprehensionExpression *expr) override;
  // match path pattern expression
  void visit(MatchPathPatternExpression *expr) override;

  Expression *andAll(const std::vector<Expression *> &exprs) const;

 private:
  ObjectPool *pool_{nullptr};
  ValidateContext *vctx_{nullptr};

  std::list<std::string> localVariables_;  // local variable defined in List Comprehension
};

}  // namespace graph
}  // namespace nebula
