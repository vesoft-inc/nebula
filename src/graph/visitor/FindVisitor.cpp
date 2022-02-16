/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "graph/visitor/FindVisitor.h"

#include <utility>  // for pair

#include "common/expression/AggregateExpression.h"          // for Aggregate...
#include "common/expression/BinaryExpression.h"             // for BinaryExp...
#include "common/expression/CaseExpression.h"               // for CaseExpre...
#include "common/expression/ColumnExpression.h"             // for ColumnExp...
#include "common/expression/ConstantExpression.h"           // for ConstantE...
#include "common/expression/ContainerExpression.h"          // for ListExpre...
#include "common/expression/EdgeExpression.h"               // for EdgeExpre...
#include "common/expression/Expression.h"                   // for Expression
#include "common/expression/FunctionCallExpression.h"       // for FunctionC...
#include "common/expression/LabelAttributeExpression.h"     // for LabelAttr...
#include "common/expression/LabelExpression.h"              // for LabelExpr...
#include "common/expression/ListComprehensionExpression.h"  // for ListCompr...
#include "common/expression/LogicalExpression.h"            // for LogicalEx...
#include "common/expression/PathBuildExpression.h"          // for PathBuild...
#include "common/expression/PredicateExpression.h"          // for Predicate...
#include "common/expression/PropertyExpression.h"           // for LabelTagP...
#include "common/expression/ReduceExpression.h"             // for ReduceExp...
#include "common/expression/SubscriptExpression.h"          // for Subscript...
#include "common/expression/TypeCastingExpression.h"        // for TypeCasti...
#include "common/expression/UUIDExpression.h"               // for UUIDExpre...
#include "common/expression/UnaryExpression.h"              // for UnaryExpr...
#include "common/expression/VariableExpression.h"           // for VariableE...
#include "common/expression/VertexExpression.h"             // for VertexExp...

namespace nebula {
namespace graph {

void FindVisitor::visit(TypeCastingExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->operand()->accept(this);
}

void FindVisitor::visit(UnaryExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->operand()->accept(this);
}

void FindVisitor::visit(FunctionCallExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  for (const auto& arg : expr->args()->args()) {
    arg->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visit(AggregateExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->arg()->accept(this);
}

void FindVisitor::visit(ListExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  for (const auto& item : expr->items()) {
    item->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visit(SetExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  for (const auto& item : expr->items()) {
    item->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visit(MapExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  for (const auto& pair : expr->items()) {
    pair.second->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visit(CaseExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;

  if (expr->hasCondition()) {
    expr->condition()->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
  if (expr->hasDefault()) {
    expr->defaultResult()->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
  for (const auto& whenThen : expr->cases()) {
    whenThen.when->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
    whenThen.then->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visit(PredicateExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;

  expr->collection()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  if (expr->hasFilter()) {
    expr->filter()->accept(this);
  }
}

void FindVisitor::visit(ReduceExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;

  expr->initial()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->collection()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->mapping()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;
}

void FindVisitor::visit(ListComprehensionExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;

  expr->collection()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;

  if (expr->hasFilter()) {
    expr->filter()->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }

  if (expr->hasMapping()) {
    expr->mapping()->accept(this);
  }
}

void FindVisitor::visit(LogicalExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  for (const auto& operand : expr->operands()) {
    operand->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visit(ConstantExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(EdgePropertyExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(TagPropertyExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(InputPropertyExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(VariablePropertyExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(SourcePropertyExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(DestPropertyExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(EdgeSrcIdExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(EdgeTypeExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(EdgeRankExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(EdgeDstIdExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(UUIDExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(VariableExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(VersionedVariableExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(LabelExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(LabelAttributeExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->left()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->right()->accept(this);
}

void FindVisitor::visit(VertexExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(LabelTagPropertyExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->label()->accept(this);
}

void FindVisitor::visit(EdgeExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(ColumnExpression* expr) {
  findInCurrentExpr(expr);
}

void FindVisitor::visit(PathBuildExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  for (const auto& item : expr->items()) {
    item->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visit(SubscriptRangeExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;

  expr->list()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;

  if (expr->lo() != nullptr) {
    expr->lo()->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }

  if (expr->hi() != nullptr) {
    expr->hi()->accept(this);
    if (!needFindAll_ && !foundExprs_.empty()) return;
  }
}

void FindVisitor::visitBinaryExpr(BinaryExpression* expr) {
  findInCurrentExpr(expr);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->left()->accept(this);
  if (!needFindAll_ && !foundExprs_.empty()) return;
  expr->right()->accept(this);
}

void FindVisitor::findInCurrentExpr(Expression* expr) {
  if (finder_(expr)) {
    foundExprs_.emplace_back(expr);
  }
}

}  // namespace graph
}  // namespace nebula
