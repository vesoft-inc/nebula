/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownScanVerticesRule.h"

#include <type_traits>  // for remove_reference...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"                     // for CheckNotNull
#include "common/expression/Expression.h"            // for Expression
#include "common/expression/LogicalExpression.h"     // for LogicalExpression
#include "graph/context/QueryContext.h"              // for QueryContext
#include "graph/optimizer/OptContext.h"              // for OptContext
#include "graph/optimizer/OptGroup.h"                // for OptGroupNode
#include "graph/planner/plan/PlanNode.h"             // for PlanNode, PlanNo...
#include "graph/planner/plan/Query.h"                // for Filter, ScanVert...
#include "graph/visitor/ExtractFilterExprVisitor.h"  // for ExtractFilterExp...

using nebula::Expression;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::ScanVertices;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownScanVerticesRule::kInstance =
    std::unique_ptr<PushFilterDownScanVerticesRule>(new PushFilterDownScanVerticesRule());

PushFilterDownScanVerticesRule::PushFilterDownScanVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushFilterDownScanVerticesRule::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kScanVertices)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownScanVerticesRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto filterGroupNode = matched.node;
  auto svGroupNode = matched.dependencies.front().node;
  auto filter = static_cast<const Filter *>(filterGroupNode->node());
  auto sv = static_cast<const ScanVertices *>(svGroupNode->node());
  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();
  auto condition = DCHECK_NOTNULL(filter->condition())->clone();

  auto visitor = graph::ExtractFilterExprVisitor::makePushGetVertices(pool);
  condition->accept(&visitor);
  if (!visitor.ok()) {
    return TransformResult::noTransform();
  }

  auto remainedExpr = std::move(visitor).remainedExpr();
  OptGroupNode *newFilterGroupNode = nullptr;
  if (remainedExpr != nullptr) {
    auto newFilter = Filter::make(qctx, nullptr, remainedExpr);
    newFilter->setOutputVar(filter->outputVar());
    newFilter->setInputVar(filter->inputVar());
    newFilterGroupNode = OptGroupNode::create(ctx, newFilter, filterGroupNode->group());
  }

  auto newSVFilter = condition;
  if (sv->filter() != nullptr) {
    auto logicExpr = LogicalExpression::makeAnd(pool, condition, sv->filter()->clone());
    newSVFilter = logicExpr;
  }

  auto newSV = static_cast<ScanVertices *>(sv->clone());
  newSV->setFilter(newSVFilter);

  OptGroupNode *newSvGroupNode = nullptr;
  if (newFilterGroupNode != nullptr) {
    // Filter(A&&B)<-ScanVertices(C) => Filter(A)<-ScanVertices(B&&C)
    auto newGroup = OptGroup::create(ctx);
    newSvGroupNode = newGroup->makeGroupNode(newSV);
    newFilterGroupNode->dependsOn(newGroup);
  } else {
    // Filter(A)<-ScanVertices(C) => ScanVertices(A&&C)
    newSvGroupNode = OptGroupNode::create(ctx, newSV, filterGroupNode->group());
    newSV->setOutputVar(filter->outputVar());
  }

  for (auto dep : svGroupNode->dependencies()) {
    newSvGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newFilterGroupNode ? newFilterGroupNode : newSvGroupNode);
  return result;
}

std::string PushFilterDownScanVerticesRule::toString() const {
  return "PushFilterDownScanVerticesRule";
}

}  // namespace opt
}  // namespace nebula
