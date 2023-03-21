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
  std::vector<Expression *> nodeOrEdgeFilters;
  auto *pathList = InputPropertyExpression::make(pool_, matchPath.toString());
  auto listElementVar = vctx_->anonVarGen()->getVar();
  for (std::size_t i = 0; i < matchPath.nodes().size(); ++i) {
    const auto &node = matchPath.nodes()[i];
    if (!node->alias().empty()) {
      const auto find = std::find(localVariables_.begin(), localVariables_.end(), node->alias());
      if (find != localVariables_.end()) {
        // TODO we should check variable is Node type
        // from local variable
        node->setVariableDefinedSource(VariableDefinedSource::kExpression);
        auto *listElement = VariableExpression::makeInner(pool_, listElementVar);
        // Note: this require build path by node pattern order
        auto *listElementId = FunctionCallExpression::make(
            pool_,
            "_nodeid",
            {listElement, ConstantExpression::make(pool_, static_cast<int64_t>(i))});
        // The alias of node is converted to a inner variable.
        // e.g. MATCH (v:player) WHERE [t in [v] | (v)-[:like]->(t)] RETURN v
        // More cases could be found in PathExprRefLocalVariable.feature
        auto *nodeValue = VariableExpression::makeInner(pool_, node->alias());
        auto *nodeId = FunctionCallExpression::make(pool_, "id", {nodeValue});
        auto *equal = RelationalExpression::makeEQ(pool_, listElementId, nodeId);
        nodeOrEdgeFilters.emplace_back(equal);
      }
    }
  }
  for (std::size_t i = 0; i < matchPath.edges().size(); ++i) {
    const auto &edge = matchPath.edges()[i];
    if (!edge->alias().empty()) {
      const auto find = std::find(localVariables_.begin(), localVariables_.end(), edge->alias());
      if (find != localVariables_.end()) {
        // TODO we should check variable is Edge type
        // from local variable
        edge->setVariableDefinedSource(VariableDefinedSource::kExpression);
        auto *listElement = VariableExpression::makeInner(pool_, listElementVar);
        // Note: this require build path by node pattern order
        auto *edgeInPath = FunctionCallExpression::make(
            pool_,
            "_edge",
            {listElement, ConstantExpression::make(pool_, static_cast<int64_t>(i))});
        // The alias of node is converted to a inner variable.
        // e.g. MATCH (v:player) WHERE [t in [v] | (v)-[:like]->(t)] RETURN v
        // More cases could be found in PathExprRefLocalVariable.feature
        auto *edgeValue = VariableExpression::makeInner(pool_, edge->alias());
        auto *equal = RelationalExpression::makeEQ(pool_, edgeInPath, edgeValue);
        nodeOrEdgeFilters.emplace_back(equal);
      }
    }
  }

  if (!nodeOrEdgeFilters.empty()) {
    auto genList = ListComprehensionExpression::make(
        pool_, listElementVar, pathList, andAll(nodeOrEdgeFilters), nullptr);
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
