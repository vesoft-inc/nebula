/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/EvaluableExprVisitor.h"

#include "graph/context/QueryContext.h"

namespace nebula {
namespace graph {

EvaluableExprVisitor::EvaluableExprVisitor(const QueryContext *qctx) : qctx_(qctx) {}

void EvaluableExprVisitor::visit(ConstantExpression *) {
  isEvaluable_ = true;
}

void EvaluableExprVisitor::visit(LabelExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(UUIDExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(VariableExpression *expr) {
  isEvaluable_ = (qctx_ && qctx_->existParameter(expr->var())) ? true : false;
}

void EvaluableExprVisitor::visit(VersionedVariableExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(TagPropertyExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(EdgePropertyExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(LabelTagPropertyExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(InputPropertyExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(VariablePropertyExpression *expr) {
  isEvaluable_ = (qctx_ && qctx_->existParameter(static_cast<PropertyExpression *>(expr)->sym()))
                     ? true
                     : false;
}

void EvaluableExprVisitor::visit(DestPropertyExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(SourcePropertyExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(EdgeSrcIdExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(EdgeTypeExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(EdgeRankExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(EdgeDstIdExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(VertexExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(EdgeExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(ColumnExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visit(SubscriptRangeExpression *) {
  isEvaluable_ = false;
}

void EvaluableExprVisitor::visitBinaryExpr(BinaryExpression *expr) {
  expr->left()->accept(this);
  if (isEvaluable_) {
    expr->right()->accept(this);
  }
}

}  // namespace graph
}  // namespace nebula
