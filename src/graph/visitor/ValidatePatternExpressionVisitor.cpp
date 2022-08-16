// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/visitor/ValidatePatternExpressionVisitor.h"

#include "ExprVisitorImpl.h"
#include "graph/context/ValidateContext.h"

namespace nebula {
namespace graph {

void ValidatePatternExpressionVisitor::visit(ListComprehensionExpression *expr) {
  DCHECK(ok()) << expr->toString();
  // Store current available variables in expression
  localVariables_.push_front(expr->innerVar());
  SCOPE_EXIT {
    localVariables_.pop_front();
  };
  ExprVisitorImpl::visit(expr);
}

void ValidatePatternExpressionVisitor::visit(MatchPathPatternExpression *expr) {
  DCHECK(ok()) << expr->toString();
  // don't need to process sub-expression
  const auto &matchPath = expr->matchPath();
  std::vector<Expression *> nodeFilters;
  auto *pathList = InputPropertyExpression::make(pool_, matchPath.toString());
  auto listElementVar = vctx_->anonVarGen()->getVar();
  for (std::size_t i = 0; i < matchPath.nodes().size(); ++i) {
    const auto &node = matchPath.nodes()[i];
    if (!node->alias().empty()) {
      const auto find = std::find(localVariables_.begin(), localVariables_.end(), node->alias());
      if (find != localVariables_.end()) {
        // TODO we should check variable is Node type
        // from local variable
        node->setVariableDefinedSource(MatchNode::VariableDefinedSource::kExpression);
        auto *listElement = VariableExpression::make(pool_, listElementVar);
        // Note: this require build path by node pattern order
        auto *listElementId = FunctionCallExpression::make(
            pool_,
            "_nodeid",
            {listElement, ConstantExpression::make(pool_, static_cast<int64_t>(i))});
        auto *nodeValue = VariableExpression::make(pool_, node->alias());
        auto *nodeId = FunctionCallExpression::make(pool_, "id", {nodeValue});
        auto *equal = RelationalExpression::makeEQ(pool_, listElementId, nodeId);
        nodeFilters.emplace_back(equal);
      }
    }
  }
  if (!nodeFilters.empty()) {
    auto genList = ListComprehensionExpression::make(
        pool_, listElementVar, pathList, andAll(nodeFilters), nullptr);
    expr->setGenList(genList);
  }
}

Expression *ValidatePatternExpressionVisitor::andAll(const std::vector<Expression *> &exprs) const {
  CHECK(!exprs.empty());
  if (exprs.size() == 1) {
    return exprs[0];
  }
  auto *expr = exprs[0];
  for (std::size_t i = 1; i < exprs.size(); ++i) {
    expr = LogicalExpression::makeAnd(pool_, expr, exprs[i]);
  }
  return expr;
}

}  // namespace graph
}  // namespace nebula
