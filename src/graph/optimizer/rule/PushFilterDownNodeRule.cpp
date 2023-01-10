/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownNodeRule.h"

#include "common/expression/Expression.h"
#include "common/expression/LogicalExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::AppendVertices;
using nebula::graph::Explore;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

/*static*/ const std::initializer_list<graph::PlanNode::Kind> PushFilterDownNodeRule::kNodes{
    graph::PlanNode::Kind::kTraverse,
    graph::PlanNode::Kind::kAppendVertices,
};

std::unique_ptr<OptRule> PushFilterDownNodeRule::kInstance =
    std::unique_ptr<PushFilterDownNodeRule>(new PushFilterDownNodeRule());

PushFilterDownNodeRule::PushFilterDownNodeRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushFilterDownNodeRule::pattern() const {
  static Pattern pattern = Pattern::create(kNodes);
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownNodeRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();
  auto *groupNode = matched.node;
  auto *node = groupNode->node();
  Expression *vFilter = nullptr;
  if (node->kind() == graph::PlanNode::Kind::kTraverse) {
    auto *traverse = static_cast<const Traverse *>(node);
    vFilter = traverse->vFilter()->clone();
  } else if (node->kind() == graph::PlanNode::Kind::kAppendVertices) {
    auto *append = static_cast<const AppendVertices *>(node);
    vFilter = append->vFilter()->clone();
  } else {
    DLOG(FATAL) << "Unsupported node kind: " << node->kind();
    return TransformResult::noTransform();
  }
  auto visitor = graph::ExtractFilterExprVisitor::makePushGetVertices(pool);
  vFilter->accept(&visitor);
  if (!visitor.ok()) {
    return TransformResult::noTransform();
  }
  auto remainedExpr = std::move(visitor).remainedExpr();
  auto *explore = static_cast<const Explore *>(node);
  auto *newExplore = static_cast<Explore *>(node->clone());
  newExplore->setOutputVar(explore->outputVar());
  auto newExploreGroupNode = OptGroupNode::create(ctx, newExplore, groupNode->group());
  if (explore->filter() != nullptr) {
    vFilter = LogicalExpression::makeAnd(pool, vFilter, newExplore->filter());
  }
  if (newExplore->kind() == graph::PlanNode::Kind::kTraverse) {
    auto *traverse = static_cast<Traverse *>(newExplore);
    traverse->setVertexFilter(remainedExpr);
    if (traverse->firstStepFilter() != nullptr) {
      vFilter = LogicalExpression::makeAnd(pool, vFilter, traverse->firstStepFilter());
    }
    traverse->setFirstStepFilter(vFilter);
  } else if (newExplore->kind() == graph::PlanNode::Kind::kAppendVertices) {
    auto *append = static_cast<AppendVertices *>(newExplore);
    append->setVertexFilter(remainedExpr);
    append->setFilter(vFilter);
  } else {
    DLOG(FATAL) << "Unsupported node kind: " << newExplore->kind();
    return TransformResult::noTransform();
  }

  newExploreGroupNode->setDeps(groupNode->dependencies());

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newExploreGroupNode);
  return result;
}

bool PushFilterDownNodeRule::match(OptContext *octx, const MatchedResult &matched) const {
  if (!OptRule::match(octx, matched)) {
    return false;
  }
  const auto *node = matched.node->node();
  if (node->kind() == graph::PlanNode::Kind::kTraverse) {
    const auto *traverse = static_cast<const graph::Traverse *>(node);
    if (traverse->vFilter() == nullptr) {
      return false;
    }
  } else if (node->kind() == graph::PlanNode::Kind::kAppendVertices) {
    const auto *append = static_cast<const graph::AppendVertices *>(node);
    if (append->vFilter() == nullptr) {
      return false;
    }
  } else {
    DLOG(FATAL) << "Unexpected node kind: " << node->kind();
    return false;
  }
  return true;
}

std::string PushFilterDownNodeRule::toString() const {
  return "PushFilterDownNodeRule";
}

}  // namespace opt
}  // namespace nebula
