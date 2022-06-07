/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushVFilterDownScanVerticesRule.h"

#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/UnaryExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::AppendVertices;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::ScanVertices;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushVFilterDownScanVerticesRule::kInstance =
    std::unique_ptr<PushVFilterDownScanVerticesRule>(new PushVFilterDownScanVerticesRule());

PushVFilterDownScanVerticesRule::PushVFilterDownScanVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushVFilterDownScanVerticesRule::pattern() const {
  static Pattern pattern = Pattern::create(PlanNode::Kind::kAppendVertices,
                                           {Pattern::create(PlanNode::Kind::kScanVertices)});
  return pattern;
}

// AppendVertices is the leaf node to fetch data from storage, so Filter can't push over it
// normally.
// But in this case, if AppendVertices get vId from ScanVertices, it can be pushed down.
bool PushVFilterDownScanVerticesRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto appendVerticesGroupNode = matched.node;
  auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  auto *src = appendVertices->src();
  if (src->kind() != Expression::Kind::kInputProperty &&
      src->kind() != Expression::Kind::kVarProperty) {
    return false;
  }
  auto *propExpr = static_cast<const PropertyExpression *>(src);
  if (propExpr->prop() != kVid) {
    return false;
  }
  if (appendVertices->vFilter() == nullptr && appendVertices->filter() == nullptr) {
    return false;
  }
  if (appendVertices->vFilter() != nullptr) {
    auto tagPropExprs = graph::ExpressionUtils::collectAll(appendVertices->vFilter(),
                                                           {Expression::Kind::kTagProperty});
    for (const auto &tagPropExpr : tagPropExprs) {
      auto tagProp = static_cast<const PropertyExpression *>(tagPropExpr);
      if (tagProp->sym() == "*") {
        return false;
      }
    }
  }
  return true;
}

StatusOr<OptRule::TransformResult> PushVFilterDownScanVerticesRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto appendVerticesGroupNode = matched.node;
  auto svGroupNode = matched.dependencies.front().node;
  auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  auto sv = static_cast<const ScanVertices *>(svGroupNode->node());
  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();
  auto newAppendVertices = appendVertices->clone();
  Expression *condition = nullptr;
  if (appendVertices->vFilter() != nullptr) {
    condition = appendVertices->vFilter()->clone();

    auto visitor = graph::ExtractFilterExprVisitor::makePushGetVertices(pool);
    condition->accept(&visitor);
    if (!visitor.ok()) {
      return TransformResult::noTransform();
    }

    auto remainedExpr = std::move(visitor).remainedExpr();
    newAppendVertices->setVertexFilter(remainedExpr);
  }
  OptGroupNode *newAppendVerticesGroupNode = nullptr;
  newAppendVertices->setOutputVar(appendVertices->outputVar());
  newAppendVerticesGroupNode =
      OptGroupNode::create(ctx, newAppendVertices, appendVerticesGroupNode->group());

  auto newSVFilter = condition;
  if (newAppendVertices->filter() != nullptr) {
    if (newSVFilter == nullptr) {
      newSVFilter = newAppendVertices->filter();
    } else {
      newSVFilter = LogicalExpression::makeAnd(pool, newAppendVertices->filter(), newSVFilter);
    }
    newAppendVertices->setFilter(nullptr);
  }
  if (sv->filter() != nullptr) {
    if (newSVFilter == nullptr) {
      newSVFilter = sv->filter()->clone();
    } else {
      newSVFilter = LogicalExpression::makeAnd(pool, newSVFilter, sv->filter()->clone());
    }
  }

  auto newSV = static_cast<ScanVertices *>(sv->clone());
  newSV->setFilter(newSVFilter);

  auto newGroup = OptGroup::create(ctx);
  auto newSvGroupNode = newGroup->makeGroupNode(newSV);
  newAppendVerticesGroupNode->dependsOn(newGroup);
  newAppendVertices->setInputVar(newSV->outputVar());

  for (auto dep : svGroupNode->dependencies()) {
    newSvGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newAppendVerticesGroupNode);
  return result;
}

std::string PushVFilterDownScanVerticesRule::toString() const {
  return "PushVFilterDownScanVerticesRule";
}

}  // namespace opt
}  // namespace nebula
