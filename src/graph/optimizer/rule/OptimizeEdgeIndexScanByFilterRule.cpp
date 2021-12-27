/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/OptimizeEdgeIndexScanByFilterRule.h"

#include "graph/context/QueryContext.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptimizerUtils.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Scan.h"
#include "graph/util/ExpressionUtils.h"

using nebula::Expression;
using nebula::graph::EdgeIndexFullScan;
using nebula::graph::EdgeIndexPrefixScan;
using nebula::graph::EdgeIndexRangeScan;
using nebula::graph::EdgeIndexScan;
using nebula::graph::Filter;
using nebula::graph::OptimizerUtils;
using nebula::graph::QueryContext;
using nebula::meta::cpp2::IndexItem;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> OptimizeEdgeIndexScanByFilterRule::kInstance =
    std::unique_ptr<OptimizeEdgeIndexScanByFilterRule>(new OptimizeEdgeIndexScanByFilterRule());

OptimizeEdgeIndexScanByFilterRule::OptimizeEdgeIndexScanByFilterRule() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& OptimizeEdgeIndexScanByFilterRule::pattern() const {
  static Pattern pattern =
      Pattern::create(Kind::kFilter, {Pattern::create(Kind::kEdgeIndexFullScan)});
  return pattern;
}

bool OptimizeEdgeIndexScanByFilterRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto scan = static_cast<const EdgeIndexFullScan*>(matched.planNode({0, 0}));
  for (auto& ictx : scan->queryContext()) {
    if (ictx.column_hints_ref().is_set()) {
      return false;
    }
  }
  auto condition = filter->condition();
  if (condition->isRelExpr()) {
    auto relExpr = static_cast<const RelationalExpression*>(condition);
    // If the container in the IN expr has only 1 element, it will be converted to an relEQ
    // expr. If more than 1 element found in the container, UnionAllIndexScanBaseRule will be
    // applied.
    if (relExpr->kind() == ExprKind::kRelIn && relExpr->right()->isContainerExpr()) {
      auto ContainerOperands = graph::ExpressionUtils::getContainerExprOperands(relExpr->right());
      return ContainerOperands.size() == 1;
    }
    return relExpr->left()->kind() == ExprKind::kEdgeProperty &&
           relExpr->right()->kind() == ExprKind::kConstant;
  }
  if (condition->isLogicalExpr()) {
    return condition->kind() == Expression::Kind::kLogicalAnd;
  }

  return false;
}

EdgeIndexScan* makeEdgeIndexScan(QueryContext* qctx, const EdgeIndexScan* scan, bool isPrefixScan) {
  EdgeIndexScan* scanNode = nullptr;
  if (isPrefixScan) {
    scanNode = EdgeIndexPrefixScan::make(qctx, nullptr, scan->edgeType());
  } else {
    scanNode = EdgeIndexRangeScan::make(qctx, nullptr, scan->edgeType());
  }
  OptimizerUtils::copyIndexScanData(scan, scanNode, qctx);
  return scanNode;
}

StatusOr<TransformResult> OptimizeEdgeIndexScanByFilterRule::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto scan = static_cast<const EdgeIndexFullScan*>(matched.planNode({0, 0}));

  auto metaClient = ctx->qctx()->getMetaClient();
  auto status = metaClient->getEdgeIndexesFromCache(scan->space());
  NG_RETURN_IF_ERROR(status);
  auto indexItems = std::move(status).value();

  OptimizerUtils::eraseInvalidIndexItems(scan->schemaId(), &indexItems);

  auto condition = filter->condition();
  auto conditionType = condition->kind();
  Expression* transformedExpr = condition->clone();

  // Stand alone IN expr with only 1 element in the list, no need to check index
  if (conditionType == ExprKind::kRelIn) {
    transformedExpr = graph::ExpressionUtils::rewriteInExpr(condition);
    DCHECK(transformedExpr->kind() == ExprKind::kRelEQ);
  }

  // case2: logical AND expr
  if (condition->kind() == ExprKind::kLogicalAnd) {
    for (auto& operand : static_cast<const LogicalExpression*>(condition)->operands()) {
      if (operand->kind() == ExprKind::kRelIn) {
        auto inExpr = static_cast<RelationalExpression*>(operand);
        // Do not apply this rule if the IN expr has a valid index or it has only 1 element in the
        // list
        if (static_cast<ListExpression*>(inExpr->right())->size() > 1) {
          return TransformResult::noTransform();
        } else {
          transformedExpr = graph::ExpressionUtils::rewriteInExpr(condition);
        }
        if (OptimizerUtils::relExprHasIndex(inExpr, indexItems)) {
          return TransformResult::noTransform();
        }
      }
    }
  }

  IndexQueryContext ictx;
  bool isPrefixScan = false;
  if (!OptimizerUtils::findOptimalIndex(transformedExpr, indexItems, &isPrefixScan, &ictx)) {
    return TransformResult::noTransform();
  }

  std::vector<IndexQueryContext> idxCtxs = {ictx};
  auto scanNode = makeEdgeIndexScan(ctx->qctx(), scan, isPrefixScan);
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

std::string OptimizeEdgeIndexScanByFilterRule::toString() const {
  return "OptimizeEdgeIndexScanByFilterRule";
}

}  // namespace opt
}  // namespace nebula
