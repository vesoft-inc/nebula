/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/TopNRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Sort;
using nebula::graph::TopN;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> TopNRule::kInstance = std::unique_ptr<TopNRule>(new TopNRule());

TopNRule::TopNRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &TopNRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kSort)});
  return pattern;
}

StatusOr<OptRule::TransformResult> TopNRule::transform(OptContext *ctx,
                                                       const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto sortGroupNode = matched.dependencies.front().node;
  auto limit = static_cast<const Limit *>(limitGroupNode->node());
  auto sort = static_cast<const Sort *>(sortGroupNode->node());

  // Currently, we cannot know the total amount of input data,
  // so only apply topn rule when offset of limit is 0
  if (limit->offset() != 0) {
    return TransformResult::noTransform();
  }

  auto qctx = ctx->qctx();
  auto topn = TopN::make(qctx, nullptr, sort->factors(), limit->offset(), limit->count(qctx));
  topn->setOutputVar(limit->outputVar());
  topn->setInputVar(sort->inputVar());
  topn->setColNames(sort->colNames());
  auto topnNode = OptGroupNode::create(ctx, topn, limitGroupNode->group());
  for (auto dep : sortGroupNode->dependencies()) {
    topnNode->dependsOn(dep);
  }

  TransformResult result;
  result.newGroupNodes.emplace_back(topnNode);
  result.eraseAll = true;
  return result;
}

std::string TopNRule::toString() const {
  return "TopNRule";
}

}  // namespace opt
}  // namespace nebula
