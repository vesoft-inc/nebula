/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/MergeGetNbrsAndProjectRule.h"

#include <ext/alloc_traits.h>  // for __alloc_traits<>::...
#include <utility>             // for move
#include <vector>              // for vector

#include "common/base/Logging.h"                   // for GetReferenceableValue
#include "common/expression/Expression.h"          // for Expression, Expres...
#include "common/expression/PropertyExpression.h"  // for InputPropertyExpre...
#include "graph/optimizer/OptGroup.h"              // for OptGroupNode
#include "graph/planner/plan/PlanNode.h"           // for PlanNode, PlanNode...
#include "graph/planner/plan/Query.h"              // for GetNeighbors, Project
#include "parser/Clauses.h"                        // for YieldColumn, Yield...

namespace nebula {
namespace opt {
class OptContext;
}  // namespace opt

namespace graph {
class QueryContext;

class QueryContext;
}  // namespace graph
namespace opt {
class OptContext;
}  // namespace opt
}  // namespace nebula

using nebula::Expression;
using nebula::InputPropertyExpression;
using nebula::graph::GetNeighbors;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> MergeGetNbrsAndProjectRule::kInstance =
    std::unique_ptr<MergeGetNbrsAndProjectRule>(new MergeGetNbrsAndProjectRule());

MergeGetNbrsAndProjectRule::MergeGetNbrsAndProjectRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &MergeGetNbrsAndProjectRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kGetNeighbors,
                                           {Pattern::create(graph::PlanNode::Kind::kProject)});
  return pattern;
}

bool MergeGetNbrsAndProjectRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  const auto *optGN = matched.node;
  auto gn = static_cast<const GetNeighbors *>(optGN->node());
  const auto *optProj = matched.dependencies.back().node;
  auto proj = static_cast<const Project *>(optProj->node());
  // The projection must project only one column which expression will be
  // assigned to the src expression of GetNeighbors
  auto srcExpr = gn->src();
  const auto &columns = proj->colNames();
  if (srcExpr->kind() != Expression::Kind::kInputProperty || columns.size() != 1UL) {
    return false;
  }
  auto inputPropExpr = static_cast<const InputPropertyExpression *>(srcExpr);
  return columns.back() == inputPropExpr->prop();
}

StatusOr<OptRule::TransformResult> MergeGetNbrsAndProjectRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  const OptGroupNode *optGN = matched.node;
  const OptGroupNode *optProj = matched.dependencies.back().node;
  DCHECK_EQ(optGN->node()->kind(), PlanNode::Kind::kGetNeighbors);
  DCHECK_EQ(optProj->node()->kind(), PlanNode::Kind::kProject);
  auto gn = static_cast<const GetNeighbors *>(optGN->node());
  auto project = static_cast<const Project *>(optProj->node());
  auto newGN = static_cast<GetNeighbors *>(gn->clone());
  auto column = project->columns()->back();
  auto srcExpr = column->expr()->clone();
  newGN->setSrc(srcExpr);
  newGN->setInputVar(project->inputVar());
  auto newOptGV = OptGroupNode::create(ctx, newGN, optGN->group());
  for (auto dep : optProj->dependencies()) {
    newOptGV->dependsOn(dep);
  }
  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newOptGV);
  return result;
}

std::string MergeGetNbrsAndProjectRule::toString() const {
  return "MergeGetNbrsAndProjectRule";
}

}  // namespace opt
}  // namespace nebula
