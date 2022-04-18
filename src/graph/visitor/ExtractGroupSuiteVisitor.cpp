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
  internalVisitor(expr);
}

void ExtractGroupSuiteVisitor::visit(RelationalExpression *expr) {
  internalVisitor(expr);
}

void ExtractGroupSuiteVisitor::visit(AggregateExpression *expr) {
  groupSuite_.groupItems.emplace_back(expr->clone());
}

void ExtractGroupSuiteVisitor::internalVisitor(Expression *expr) {
  if (ExpressionUtils::hasAny(expr, {Expression::Kind::kAggregate})) {
    ExprVisitorImpl::visit(expr);
  } else {
    pushGroupSuite(expr);
  }
}

void ExtractGroupSuiteVisitor::pushGroupSuite(Expression *expr) {
  groupSuite_.groupKeys.emplace_back(expr->clone());
  groupSuite_.groupItems.emplace_back(expr->clone());
}

}  // namespace graph
}  // namespace nebula
