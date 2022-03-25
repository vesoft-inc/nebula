/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/OptimizeTagIndexScanByFilterRule.h"

#include "graph/context/QueryContext.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptimizerUtils.h"
#include "graph/optimizer/rule/IndexScanRule.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Scan.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Filter;
using nebula::graph::OptimizerUtils;
using nebula::graph::QueryContext;
using nebula::graph::TagIndexFullScan;
using nebula::graph::TagIndexPrefixScan;
using nebula::graph::TagIndexRangeScan;
using nebula::graph::TagIndexScan;
using nebula::storage::cpp2::IndexColumnHint;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> OptimizeTagIndexScanByFilterRule::kInstance =
    std::unique_ptr<OptimizeTagIndexScanByFilterRule>(new OptimizeTagIndexScanByFilterRule());

OptimizeTagIndexScanByFilterRule::OptimizeTagIndexScanByFilterRule() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& OptimizeTagIndexScanByFilterRule::pattern() const {
  static Pattern pattern =
      Pattern::create(Kind::kFilter, {Pattern::create(Kind::kTagIndexFullScan)});
  return pattern;
}

// Match 2 kinds of expressions:
//
// 1. Relational expr. If it is an IN expr, its list MUST have only 1 element, so it could always be
// transformed to an relEQ expr. i.g.  A in [B]  =>  A == B
// It the list has more than 1 element, the expr will be matched with UnionAllIndexScanBaseRule.
//
// 2. Logical AND expr. If the AND expr contains an operand that is an IN expr, the label attribute
// in the IN expr SHOULD NOT have a valid index, otherwise the expression should be matched with
// UnionAllIndexScanBaseRule.
bool OptimizeTagIndexScanByFilterRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto scan = static_cast<const TagIndexFullScan*>(matched.planNode({0, 0}));
  for (auto& ictx : scan->queryContext()) {
    if (ictx.column_hints_ref().is_set()) {
      return false;
    }
  }
  auto condition = filter->condition();

  // Case1: relational expr
  if (condition->isRelExpr()) {
    auto relExpr = static_cast<const RelationalExpression*>(condition);
    // If the container in the IN expr has only 1 element, it will be converted to an relEQ
    // expr. If more than 1 element found in the container, UnionAllIndexScanBaseRule will be
    // applied.
    if (relExpr->kind() == ExprKind::kRelIn && relExpr->right()->isContainerExpr()) {
      auto ContainerOperands = graph::ExpressionUtils::getContainerExprOperands(relExpr->right());
      return ContainerOperands.size() == 1;
    }
    return relExpr->left()->kind() == ExprKind::kTagProperty &&
           relExpr->right()->kind() == ExprKind::kConstant;
  }

  // Case2: logical AND expr
  return condition->kind() == ExprKind::kLogicalAnd;
}

TagIndexScan* makeTagIndexScan(QueryContext* qctx, const TagIndexScan* scan, bool isPrefixScan) {
  TagIndexScan* tagScan = nullptr;
  if (isPrefixScan) {
    tagScan = TagIndexPrefixScan::make(qctx, nullptr, scan->tagName());
  } else {
    tagScan = TagIndexRangeScan::make(qctx, nullptr, scan->tagName());
  }

  OptimizerUtils::copyIndexScanData(scan, tagScan, qctx);
  return tagScan;
}

StatusOr<TransformResult> OptimizeTagIndexScanByFilterRule::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto scan = static_cast<const TagIndexFullScan*>(matched.planNode({0, 0}));

  auto metaClient = ctx->qctx()->getMetaClient();
  auto status = metaClient->getTagIndexesFromCache(scan->space());
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
  size_t operandIndex = 0;
  if (conditionType == ExprKind::kLogicalAnd) {
    for (auto& operand : static_cast<const LogicalExpression*>(condition)->operands()) {
      if (operand->kind() == ExprKind::kRelIn) {
        auto inExpr = static_cast<RelationalExpression*>(operand);
        // Do not apply this rule if the IN expr has a valid index or it has more than 1 element in
        // the list
        if (static_cast<ListExpression*>(inExpr->right())->size() > 1) {
          return TransformResult::noTransform();
        } else {
          // If the inner IN expr has only 1 element, rewrite it to an relEQ expression and there is
          // no need to check whether it has a index
          auto relEqExpr = graph::ExpressionUtils::rewriteInExpr(inExpr);
          static_cast<LogicalExpression*>(transformedExpr)->setOperand(operandIndex, relEqExpr);
          continue;
        }
        if (OptimizerUtils::relExprHasIndex(inExpr, indexItems)) {
          return TransformResult::noTransform();
        }
      }
      ++operandIndex;
    }
  }

  IndexQueryContext ictx;
  bool isPrefixScan = false;
  if (!OptimizerUtils::findOptimalIndex(transformedExpr, indexItems, &isPrefixScan, &ictx)) {
    return TransformResult::noTransform();
  }

  std::vector<IndexQueryContext> idxCtxs = {ictx};
  auto scanNode = makeTagIndexScan(ctx->qctx(), scan, isPrefixScan);
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

std::string OptimizeTagIndexScanByFilterRule::toString() const {
  return "OptimizeTagIndexScanByFilterRule";
}

}  // namespace opt
}  // namespace nebula
