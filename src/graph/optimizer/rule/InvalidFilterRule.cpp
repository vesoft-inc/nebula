/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/InvalidFilterRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Filter;
using nebula::graph::PlanNode;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> InvalidFilterRule::kInstance =
    std::unique_ptr<InvalidFilterRule>(new InvalidFilterRule());

InvalidFilterRule::InvalidFilterRule() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& InvalidFilterRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter);
  return pattern;
}

bool InvalidFilterRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  const auto* filterGroupNode = matched.node;
  const auto* filter = filterGroupNode->node();
  DCHECK_EQ(filter->kind(), PlanNode::Kind::kFilter);
  if (static_cast<const graph::Filter*>(filter)->alwaysFalse()) {
    return true;
  }

  const auto* condition = static_cast<const graph::Filter*>(filter)->condition();
  return isAlwaysFalse(ctx, condition);
}

StatusOr<OptRule::TransformResult> InvalidFilterRule::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  auto filterGroupNode = matched.node;
  if (matched.dependencies.empty()) {
    return TransformResult::noTransform();
  }
  auto depGroupNode = matched.dependencies.front().node;
  auto depNode = depGroupNode->node();
  if (depNode->isBiInput()) {
    return TransformResult::noTransform();
  }

  OptGroupNode* newDepGroupNode = nullptr;
  auto newDepNode = static_cast<decltype(depNode)>(depNode->clone());
  const auto filter = static_cast<const Filter*>(filterGroupNode->node());

  if (depNode->kind() == PlanNode::Kind::kIndexScan ||
      depNode->kind() == PlanNode::Kind::kScanVertices) {
    // Filter(alwaysFalse)<-IndexScan => IndexScan(alwaysFalse)
    // Filter(alwaysFalse)<-scanVertices => scanVertices(alwaysFalse)
    newDepNode->setOutputVar(filter->outputVar());
    if (depNode->kind() == PlanNode::Kind::kIndexScan) {
      static_cast<graph::IndexScan*>(newDepNode)->setAlwaysFalse();
    } else {
      static_cast<graph::ScanVertices*>(newDepNode)->setAlwaysFalse();
    }
    newDepGroupNode = OptGroupNode::create(ctx, newDepNode, filterGroupNode->group());

    for (auto dep : depGroupNode->dependencies()) {
      newDepGroupNode->dependsOn(dep);
    }
  } else {
    // Filter(alwaysFalse)<-dep => dep<-Filter(alwaysFalse)
    auto newFilter = static_cast<Filter*>(filter->clone());
    newFilter->setAlwaysFalse();

    auto newFilterGroup = OptGroup::create(ctx);
    auto depInputVar = depNode->inputVar();

    newFilter->setInputVar(depInputVar);
    auto* varPtr = ctx->qctx()->symTable()->getVar(depInputVar);
    DCHECK(!!varPtr);
    newFilter->setColNames(varPtr->colNames);
    auto newFilterGroupNode = newFilterGroup->makeGroupNode(newFilter);

    newDepNode->setOutputVar(filter->outputVar());
    newDepNode->setInputVar(newFilter->outputVar());
    newDepGroupNode = OptGroupNode::create(ctx, newDepNode, filterGroupNode->group());

    newDepGroupNode->dependsOn(const_cast<OptGroup*>(newFilterGroupNode->group()));
    for (auto dep : depGroupNode->dependencies()) {
      newFilterGroupNode->dependsOn(dep);
    }
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newDepGroupNode);
  return result;
}

std::string InvalidFilterRule::toString() const {
  return "InvalidFilterRule";
}

bool InvalidFilterRule::isAlwaysFalse(OptContext* ctx, const Expression* filterExpr) const {
  // check v.tag.noexist == 'xxx' / v.noexist.prop == 'xxx'
  // v.tag.noexist == 'xxx' AND v.tag.prop == 'yyy'
  if (filterExpr->kind() != Expression::Kind::kRelEQ) {
    return false;
  }
  auto qctx = ctx->qctx();
  auto spaceID = qctx->rctx()->session()->space().id;
  auto exprs =
      graph::ExpressionUtils::collectAll(filterExpr, {Expression::Kind::kLabelTagProperty});
  for (auto expr : exprs) {
    const auto propExr = static_cast<const PropertyExpression*>(expr);
    const auto& tag = propExr->sym();
    auto tagId = qctx->schemaMng()->toTagID(spaceID, tag);
    if (!tagId.ok()) {
      return true;
    }
    auto schema = qctx->schemaMng()->getTagSchema(spaceID, tagId.value());
    if (!schema) {
      return true;
    }
    const auto& prop = propExr->prop();
    auto* field = schema->field(prop);
    if (field == nullptr) {
      return true;
    }
  }
  return false;
}
}  // namespace opt
}  // namespace nebula
