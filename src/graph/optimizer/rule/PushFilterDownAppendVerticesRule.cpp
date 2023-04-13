/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownAppendVerticesRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::AppendVertices;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownAppendVerticesRule::kInstance =
    std::unique_ptr<PushFilterDownAppendVerticesRule>(new PushFilterDownAppendVerticesRule());

PushFilterDownAppendVerticesRule::PushFilterDownAppendVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushFilterDownAppendVerticesRule::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kAppendVertices)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownAppendVerticesRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto filterGroupNode = matched.node;
  auto appendVerticesGroupNode = matched.dependencies.front().node;
  auto filter = static_cast<const Filter *>(filterGroupNode->node());
  auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();
  auto condition = graph::ExpressionUtils::rewriteVertexPropertyFilter(
      pool, appendVertices->colNames().back(), filter->condition()->clone());

  auto visitor = graph::ExtractFilterExprVisitor::makePushGetVertices(
      pool, appendVertices->space(), qctx->schemaMng());
  condition->accept(&visitor);
  if (!visitor.ok()) {
    return TransformResult::noTransform();
  }

  auto remainedExpr = std::move(visitor).remainedExpr();
  OptGroupNode *newFilterGroupNode = nullptr;
  PlanNode *newFilter = nullptr;
  if (remainedExpr != nullptr) {
    auto *found = graph::ExpressionUtils::findAny(remainedExpr, {Expression::Kind::kTagProperty});
    if (found != nullptr) {  // Some tag property expression don't push down
      // TODO(shylock): we could push down a part.
      return TransformResult::noTransform();
    }
    newFilter = Filter::make(qctx, nullptr, remainedExpr);
    newFilter->setOutputVar(filter->outputVar());
    newFilter->setInputVar(filter->inputVar());
    newFilterGroupNode = OptGroupNode::create(ctx, newFilter, filterGroupNode->group());
  }

  auto newAppendVerticesFilter = condition;
  if (condition->isLogicalExpr()) {
    const auto *le = static_cast<const LogicalExpression *>(condition);
    if (le->operands().size() == 1) {
      newAppendVerticesFilter = le->operands().front();
    }
  }
  if (appendVertices->filter() != nullptr) {
    auto logicExpr = LogicalExpression::makeAnd(
        pool, newAppendVerticesFilter, appendVertices->filter()->clone());
    newAppendVerticesFilter = logicExpr;
  }

  auto newAppendVertices = static_cast<AppendVertices *>(appendVertices->clone());
  newAppendVertices->setFilter(newAppendVerticesFilter);

  OptGroupNode *newAppendVerticesGroupNode = nullptr;
  if (newFilterGroupNode != nullptr) {
    // Filter(A&&B)<-AppendVertices(C) => Filter(A)<-AppendVertices(B&&C)
    auto newGroup = OptGroup::create(ctx);
    newAppendVerticesGroupNode = newGroup->makeGroupNode(newAppendVertices);
    newFilterGroupNode->dependsOn(newGroup);
    newFilter->setInputVar(newAppendVertices->outputVar());
  } else {
    // Filter(A)<-AppendVertices(C) => AppendVertices(A&&C)
    newAppendVerticesGroupNode =
        OptGroupNode::create(ctx, newAppendVertices, filterGroupNode->group());
    newAppendVertices->setOutputVar(filter->outputVar());
    newAppendVertices->setColNames(appendVertices->colNames());
  }

  for (auto dep : appendVerticesGroupNode->dependencies()) {
    newAppendVerticesGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newFilterGroupNode ? newFilterGroupNode
                                                       : newAppendVerticesGroupNode);
  return result;
}

std::string PushFilterDownAppendVerticesRule::toString() const {
  return "PushFilterDownAppendVerticesRule";
}

}  // namespace opt
}  // namespace nebula
