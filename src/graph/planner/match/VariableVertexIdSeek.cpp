/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/VariableVertexIdSeek.h"

#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

namespace nebula {
namespace graph {

bool VariableVertexIdSeek::matchNode(NodeContext *nodeCtx) {
  auto whereClause = nodeCtx->bindWhereClause;
  if (!whereClause || !whereClause->filter) {
    return false;
  }

  auto nodeInfo = nodeCtx->info;
  if (nodeInfo->alias.empty() || nodeInfo->anonymous) {
    // require one named node
    return false;
  }

  Expression *vidPredicate = whereClause->filter;
  std::string refVarName;
  if (!extractVidPredicate(nodeInfo->alias, vidPredicate, &refVarName)) {
    return false;
  }

  // exclude the case where `refVarName` is from the path pattern of current match
  if (!nodeCtx->nodeAliasesAvailable->count(refVarName)) {
    return false;
  }

  nodeCtx->refVarName = refVarName;
  return true;
}

StatusOr<SubPlan> VariableVertexIdSeek::transformNode(NodeContext *nodeCtx) {
  auto *qctx = nodeCtx->qctx;
  const auto &refVarName = nodeCtx->refVarName;
  DCHECK(!refVarName.empty());

  SubPlan plan;
  auto argument = Argument::make(qctx, refVarName);
  argument->setColNames({refVarName});
  argument->setInputVertexRequired(false);
  plan.root = plan.tail = argument;

  nodeCtx->initialExpr = InputPropertyExpression::make(qctx->objPool(), refVarName);
  return plan;
}

bool VariableVertexIdSeek::extractVidPredicate(const std::string &nodeAlias,
                                               Expression *filter,
                                               std::string *var) {
  switch (filter->kind()) {
    case Expression::Kind::kRelEQ:
    case Expression::Kind::kRelIn: {
      return isVidPredicate(nodeAlias, filter, var);
    }
    case Expression::Kind::kLogicalAnd: {
      filter = filter->clone();
      ExpressionUtils::pullAnds(filter);
      auto logicAndExpr = static_cast<LogicalExpression *>(filter);
      for (auto operand : logicAndExpr->operands()) {
        if (isVidPredicate(nodeAlias, operand, var)) {
          return true;
        }
      }
      return false;
    }
    default: {
      return false;
    }
  }
}

bool VariableVertexIdSeek::isVidPredicate(const std::string &nodeAlias,
                                          const Expression *filter,
                                          std::string *var) {
  if (filter->kind() != Expression::Kind::kRelEQ && filter->kind() != Expression::Kind::kRelIn) {
    return false;
  }

  auto relExpr = static_cast<const RelationalExpression *>(filter);
  auto checkFunCall = [var, &nodeAlias](const Expression *expr, const Expression *varExpr) {
    if (isIdFunCallExpr(nodeAlias, expr) && varExpr->kind() == Expression::Kind::kLabel) {
      *var = static_cast<const LabelExpression *>(varExpr)->name();
      return true;
    }
    return false;
  };

  if (relExpr->left()->kind() == Expression::Kind::kFunctionCall) {
    return checkFunCall(relExpr->left(), relExpr->right());
  }

  if (relExpr->right()->kind() == Expression::Kind::kFunctionCall) {
    return checkFunCall(relExpr->right(), relExpr->left());
  }

  return false;
}

bool VariableVertexIdSeek::isIdFunCallExpr(const std::string &nodeAlias, const Expression *filter) {
  if (filter->kind() == Expression::Kind::kFunctionCall) {
    auto funCallExpr = static_cast<const FunctionCallExpression *>(filter);
    if (funCallExpr->name() == "id") {
      DCHECK_EQ(funCallExpr->args()->numArgs(), 1u);
      auto arg = funCallExpr->args()->args()[0];
      if (arg->kind() == Expression::Kind::kLabel) {
        auto labelExpr = static_cast<const LabelExpression *>(arg);
        if (labelExpr->name() == nodeAlias) {
          return true;
        }
      }
    }
  }
  return false;
}

}  // namespace graph
}  // namespace nebula
