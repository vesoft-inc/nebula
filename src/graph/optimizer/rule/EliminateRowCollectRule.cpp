/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/EliminateRowCollectRule.h"

#include <utility>  // for move
#include <vector>   // for vector

#include "graph/optimizer/OptGroup.h"     // for OptGroupNode
#include "graph/planner/plan/PlanNode.h"  // for PlanNode, PlanNode::Kind
#include "graph/planner/plan/Query.h"     // for DataCollect, Project, DataC...

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

using nebula::graph::DataCollect;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> EliminateRowCollectRule::kInstance =
    std::unique_ptr<EliminateRowCollectRule>(new EliminateRowCollectRule());

EliminateRowCollectRule::EliminateRowCollectRule() {
  RuleSet::QueryRules().addRule(this);
}

// TODO match DataCollect->(Any Node with real result)
const Pattern& EliminateRowCollectRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kDataCollect,
                                           {Pattern::create(graph::PlanNode::Kind::kProject)});
  return pattern;
}

bool EliminateRowCollectRule::match(OptContext* octx, const MatchedResult& matched) const {
  if (!OptRule::match(octx, matched)) {
    return false;
  }
  const auto* collectNode = static_cast<const DataCollect*>(matched.node->node());
  if (collectNode->kind() != DataCollect::DCKind::kRowBasedMove) {
    return false;
  }
  return true;
}

StatusOr<OptRule::TransformResult> EliminateRowCollectRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto dataCollectGroupNode = matched.node;
  auto projGroupNode = matched.dependencies.front().node;

  const auto dataCollect = static_cast<const DataCollect*>(dataCollectGroupNode->node());
  const auto proj = static_cast<const Project*>(projGroupNode->node());

  auto newProj = static_cast<Project*>(proj->clone());
  newProj->setOutputVar(dataCollect->outputVar());
  auto newProjGroupNode = OptGroupNode::create(octx, newProj, dataCollectGroupNode->group());

  for (auto dep : projGroupNode->dependencies()) {
    newProjGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newProjGroupNode);
  return result;
}

std::string EliminateRowCollectRule::toString() const {
  return "EliminateRowCollectRule";
}

}  // namespace opt
}  // namespace nebula
