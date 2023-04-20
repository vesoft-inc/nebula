/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterThroughAppendVerticesRule.h"

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

std::unique_ptr<OptRule> PushFilterThroughAppendVerticesRule::kInstance =
    std::unique_ptr<PushFilterThroughAppendVerticesRule>(new PushFilterThroughAppendVerticesRule());

PushFilterThroughAppendVerticesRule::PushFilterThroughAppendVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterThroughAppendVerticesRule::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kAppendVertices)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterThroughAppendVerticesRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* oldFilterGroupNode = matched.node;
  auto* oldFilterGroup = oldFilterGroupNode->group();
  auto* oldFilterNode = static_cast<graph::Filter*>(oldFilterGroupNode->node());
  auto* condition = oldFilterNode->condition();
  auto* oldAvGroupNode = matched.dependencies[0].node;
  auto* oldAvNode = static_cast<graph::AppendVertices*>(oldAvGroupNode->node());
  auto& dstNode = oldAvNode->nodeAlias();

  auto inputColNames = oldAvNode->inputVars().front()->colNames;
  auto qctx = octx->qctx();

  auto picker = [&inputColNames, &dstNode](const Expression* expr) -> bool {
    auto finder = [&inputColNames, &dstNode](const Expression* e) -> bool {
      if (e->isPropertyExpr() &&
          std::find(inputColNames.begin(),
                    inputColNames.end(),
                    (static_cast<const PropertyExpression*>(e)->prop())) == inputColNames.end()) {
        return true;
      }
      if (e->kind() == Expression::Kind::kVar &&
          static_cast<const VariableExpression*>(e)->var() == dstNode) {
        return true;
      }
      return false;
    };
    graph::FindVisitor visitor(finder);
    const_cast<Expression*>(expr)->accept(&visitor);
    return visitor.results().empty();
  };
  Expression* filterPicked = nullptr;
  Expression* filterUnpicked = nullptr;
  graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, &filterUnpicked);

  if (!filterPicked) {
    return TransformResult::noTransform();
  }

  // produce new Filter node below
  auto* newBelowFilterNode =
      graph::Filter::make(qctx, const_cast<graph::PlanNode*>(oldAvNode->dep()), filterPicked);
  newBelowFilterNode->setInputVar(oldAvNode->inputVar());
  newBelowFilterNode->setColNames(oldAvNode->inputVars().front()->colNames);
  auto newBelowFilterGroup = OptGroup::create(octx);
  auto newFilterGroupNode = newBelowFilterGroup->makeGroupNode(newBelowFilterNode);
  newFilterGroupNode->setDeps(oldAvGroupNode->dependencies());

  // produce new AppendVertices node
  auto* newAvNode = static_cast<graph::AppendVertices*>(oldAvNode->clone());
  newAvNode->setInputVar(newBelowFilterNode->outputVar());

  TransformResult result;
  result.eraseAll = true;
  if (filterUnpicked) {
    // produce new Filter node above
    auto* newAboveFilterNode = graph::Filter::make(octx->qctx(), newAvNode, filterUnpicked);
    newAboveFilterNode->setOutputVar(oldFilterNode->outputVar());
    auto newAboveFilterGroupNode = OptGroupNode::create(octx, newAboveFilterNode, oldFilterGroup);

    auto newAvGroup = OptGroup::create(octx);
    auto newAvGroupNode = newAvGroup->makeGroupNode(newAvNode);
    newAvGroupNode->setDeps({newBelowFilterGroup});
    newAvNode->setInputVar(newBelowFilterNode->outputVar());
    newAboveFilterGroupNode->setDeps({newAvGroup});
    newAboveFilterNode->setInputVar(newAvNode->outputVar());
    result.newGroupNodes.emplace_back(newAboveFilterGroupNode);
  } else {
    newAvNode->setOutputVar(oldFilterNode->outputVar());
    // newAvNode's col names, on the hand, should inherit those of the oldAvNode
    // since they are the same project.
    newAvNode->setColNames(oldAvNode->outputVarPtr()->colNames);
    auto newAvGroupNode = OptGroupNode::create(octx, newAvNode, oldFilterGroup);
    newAvGroupNode->setDeps({newBelowFilterGroup});
    newAvNode->setInputVar(newBelowFilterNode->outputVar());
    result.newGroupNodes.emplace_back(newAvGroupNode);
  }
  return result;
}

std::string PushFilterThroughAppendVerticesRule::toString() const {
  return "PushFilterThroughAppendVerticesRule";
}

}  // namespace opt
}  // namespace nebula
