/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/ExtractGroupSuiteVisitor.h"

#include "graph/util/ExpressionUtils.h"

namespace nebula {
namespace graph {

void ExtractGroupSuiteVisitor::visit(ConstantExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(UnaryExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(TypeCastingExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(LabelExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(LabelAttributeExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(ArithmeticExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(RelationalExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(SubscriptExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(AttributeExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(LogicalExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(FunctionCallExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(AggregateExpression *expr) {
  groupSuite_.groupItems.emplace_back(expr->clone());
}

void ExtractGroupSuiteVisitor::visit(UUIDExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(VariableExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(VersionedVariableExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(ListExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(SetExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(MapExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(LabelTagPropertyExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(TagPropertyExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(EdgePropertyExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(InputPropertyExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(VariablePropertyExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(DestPropertyExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(SourcePropertyExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(EdgeSrcIdExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(EdgeTypeExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(EdgeRankExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(EdgeDstIdExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(VertexExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(EdgeExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(CaseExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(PathBuildExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(ColumnExpression *expr) {
  pushGroupSuite(expr);
}

void ExtractGroupSuiteVisitor::visit(PredicateExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(ListComprehensionExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(ReduceExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(SubscriptRangeExpression *expr) {
  internalVisit(expr);
}

void ExtractGroupSuiteVisitor::visit(MatchPathPatternExpression *expr) {
  internalVisit(expr);
}

template <typename T>
void ExtractGroupSuiteVisitor::internalVisit(T *expr) {
  if (ExpressionUtils::hasAny(expr, {Expression::Kind::kAggregate})) {
    ExprVisitorImpl::visit(expr);
  } else {
    if (!ExpressionUtils::isEvaluableExpr(expr, qctx_)) {
      pushGroupSuite(expr);
    }
  }
}

template <typename T>
void ExtractGroupSuiteVisitor::pushGroupSuite(T *expr) {
  // When expr is PredicateExpression, ListComprehensionExpression or ReduceExpression,
  // it needs to check whether contains innerVar. If so, it doesn't need to push groupKeys and
  // groupItems.
  auto specialExprs = ExpressionUtils::collectAll(expr, {Expression::Kind::kVar});
  for (auto *s : specialExprs) {
    if (s->kind() == Expression::Kind::kVar &&
        static_cast<const VariableExpression *>(s)->isInner()) {
      return;
    }
  }
  if (ExpressionUtils::isEvaluableExpr(expr, qctx_)) {
    return;
  }
  groupSuite_.groupKeys.emplace_back(expr->clone());
  groupSuite_.groupItems.emplace_back(expr->clone());
}

}  // namespace graph
}  // namespace nebula
