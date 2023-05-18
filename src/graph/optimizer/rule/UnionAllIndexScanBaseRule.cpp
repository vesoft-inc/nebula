/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/UnionAllIndexScanBaseRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/planner/plan/Scan.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/OptimizerUtils.h"

using nebula::graph::Filter;
using nebula::graph::IndexScan;
using nebula::graph::OptimizerUtils;
using nebula::graph::TagIndexFullScan;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

// The matched expression should be either a OR expression or an expression that could be
// rewrote to a OR expression. There are 3 scenarios.
//
// 1. OR expr. If OR expr has an IN expr operand that has a valid index, expand it to OR expr.
//
// 2. AND expr such as A in [a, b] AND B when A has a valid index, because it can be transformed to
// (A==a AND B) OR (A==b AND B)
//
// 3. IN expr with its list size > 1, such as A in [a, b] since it can be transformed to (A==a) OR
// (A==b).
// If the list has a size of 1, the expr will be matched with OptimizeTagIndexScanByFilterRule.
bool UnionAllIndexScanBaseRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto scan = static_cast<const IndexScan*>(matched.planNode({0, 0}));
  auto condition = filter->condition();
  auto conditionType = condition->kind();

  if (condition->isLogicalExpr()) {
    // Don't support XOR yet
    if (conditionType == ExprKind::kLogicalXor) return false;
    if (graph::ExpressionUtils::findAny(static_cast<LogicalExpression*>(condition),
                                        {ExprKind::kLogicalXor}))
      return false;
    if (conditionType == ExprKind::kLogicalOr) return true;
    if (conditionType == ExprKind::kLogicalAnd &&
        graph::ExpressionUtils::findAny(static_cast<LogicalExpression*>(condition),
                                        {ExprKind::kRelIn})) {
      return true;
    }
    // Check logical operands
    for (auto operand : static_cast<const LogicalExpression*>(condition)->operands()) {
      if (!operand->isRelExpr() || !operand->isLogicalExpr()) {
        return false;
      }
    }
  }

  // If the number of elements is less or equal than 1, the IN expr will be transformed into a
  // relEQ expr by the OptimizeTagIndexScanByFilterRule.
  if (condition->isRelExpr()) {
    auto relExpr = static_cast<const RelationalExpression*>(condition);
    auto* rhs = relExpr->right();
    if (relExpr->kind() == ExprKind::kRelIn) {
      if (rhs->isContainerExpr()) {
        return graph::ExpressionUtils::getContainerExprOperands(rhs).size() > 1;
      } else if (rhs->kind() == Expression::Kind::kConstant) {
        auto constExprValue = static_cast<const ConstantExpression*>(rhs)->value();
        return (constExprValue.isList() && constExprValue.getList().size() > 1) ||
               (constExprValue.isSet() && constExprValue.getSet().size() > 1);
      }
    }
  }

  for (auto& ictx : scan->queryContext()) {
    if (ictx.column_hints_ref().is_set()) {
      return false;
    }
  }

  return false;
}

StatusOr<TransformResult> UnionAllIndexScanBaseRule::transform(OptContext* ctx,
                                                               const MatchedResult& matched) const {
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto node = matched.planNode({0, 0});
  auto scan = static_cast<const IndexScan*>(node);
  auto* qctx = ctx->qctx();
  auto metaClient = qctx->getMetaClient();
  auto status = node->kind() == graph::PlanNode::Kind::kTagIndexFullScan
                    ? metaClient->getTagIndexesFromCache(scan->space())
                    : metaClient->getEdgeIndexesFromCache(scan->space());

  NG_RETURN_IF_ERROR(status);
  auto indexItems = std::move(status).value();

  OptimizerUtils::eraseInvalidIndexItems(scan->schemaId(), &indexItems);

  // Check whether the prop has index.
  // Rewrite if the property in the IN expr has a valid index
  if (indexItems.empty()) {
    return TransformResult::noTransform();
  }

  auto condition = filter->condition();
  auto conditionType = condition->kind();
  Expression* transformedExpr = condition->clone();

  switch (conditionType) {
    // Stand alone IN expr
    // If it has multiple elements in the list, check valid index before expanding to OR expr
    case ExprKind::kRelIn: {
      if (!OptimizerUtils::relExprHasIndex(condition, indexItems)) {
        return TransformResult::noTransform();
      }
      transformedExpr = graph::ExpressionUtils::rewriteInExpr(condition);
      break;
    }

    // AND expr containing IN expr operand
    case ExprKind::kLogicalAnd: {
      // Iterate all operands and expand IN exprs if possible
      for (auto& expr : static_cast<LogicalExpression*>(transformedExpr)->operands()) {
        if (expr->kind() == ExprKind::kRelIn) {
          if (OptimizerUtils::relExprHasIndex(expr, indexItems)) {
            expr = graph::ExpressionUtils::rewriteInExpr(expr);
          }
        }
      }

      // Reconstruct AND expr using distributive law
      transformedExpr = graph::ExpressionUtils::rewriteLogicalAndToLogicalOr(transformedExpr);

      // If there is no OR expr in the transformedExpr, only one index scan should be done.
      // OptimizeTagIndexScanByFilterRule should be applied
      if (!graph::ExpressionUtils::findAny(transformedExpr, {Expression::Kind::kLogicalOr})) {
        return TransformResult::noTransform();
      }
      break;
    }

    // OR expr
    case ExprKind::kLogicalOr: {
      // Iterate all operands and expand IN exprs if possible
      for (auto& expr : static_cast<LogicalExpression*>(transformedExpr)->operands()) {
        if (expr->kind() == ExprKind::kRelIn) {
          if (OptimizerUtils::relExprHasIndex(expr, indexItems)) {
            expr = graph::ExpressionUtils::rewriteInExpr(expr);
          }
        }
      }
      // Flatten OR exprs
      graph::ExpressionUtils::pullOrs(transformedExpr);

      break;
    }
    default:
      DLOG(ERROR) << "Invalid expression kind: " << static_cast<uint8_t>(conditionType);
      return TransformResult::noTransform();
  }

  DCHECK(transformedExpr->kind() == ExprKind::kLogicalOr);
  std::vector<IndexQueryContext> idxCtxs;
  auto logicalExpr = static_cast<const LogicalExpression*>(transformedExpr);
  for (auto operand : logicalExpr->operands()) {
    IndexQueryContext ictx;
    bool isPrefixScan = false;
    if (!OptimizerUtils::findOptimalIndex(operand, indexItems, &isPrefixScan, &ictx)) {
      return TransformResult::noTransform();
    }
    idxCtxs.emplace_back(std::move(ictx));
  }

  auto scanNode = IndexScan::make(qctx, nullptr);
  OptimizerUtils::copyIndexScanData(scan, scanNode, qctx);
  scanNode->setIndexQueryContext(std::move(idxCtxs));
  scanNode->setOutputVar(filter->outputVar());
  scanNode->setColNames(filter->colNames());
  auto filterGroup = matched.node->group();
  auto optScanNode = OptGroupNode::create(ctx, scanNode, filterGroup);
  for (auto group : matched.dependencies[0].node->dependencies()) {
    optScanNode->dependsOn(group);
  }
  TransformResult result;
  result.newGroupNodes.emplace_back(optScanNode);
  result.eraseCurr = true;
  return result;
}

}  // namespace opt
}  // namespace nebula
