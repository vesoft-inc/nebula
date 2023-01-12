/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/GetEdgesTransformRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/rule/GetEdgesTransformUtils.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::ScanEdges;
using nebula::graph::ScanVertices;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> GetEdgesTransformRule::kInstance =
    std::unique_ptr<GetEdgesTransformRule>(new GetEdgesTransformRule());

GetEdgesTransformRule::GetEdgesTransformRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &GetEdgesTransformRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kProject,
      {Pattern::create({PlanNode::Kind::kLimit},
                       {Pattern::create(PlanNode::Kind::kTraverse,
                                        {Pattern::create(PlanNode::Kind::kScanVertices)})})});
  return pattern;
}

bool GetEdgesTransformRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto traverse = static_cast<const Traverse *>(matched.planNode({0, 0, 0}));
  auto project = static_cast<const Project *>(matched.planNode({0}));
  const auto &colNames = traverse->colNames();
  auto colSize = colNames.size();
  DCHECK_GE(colSize, 2);
  // TODO: Poor readability for optimizer, is there any other way to do the same thing?
  if (colNames[colSize - 2][0] != '_') {  // src
    return false;
  }
  const auto &stepRange = traverse->stepRange();
  if (stepRange.min() != 1 || stepRange.max() != 1) {
    return false;
  }
  // Can't apply vertex filter in GetEdges
  if (traverse->vFilter() != nullptr) {
    return false;
  }
  // If edge filter is not null, means it's can't be pushed down to storage
  if (traverse->eFilter() != nullptr) {
    return false;
  }
  for (auto yieldColumn : project->columns()->columns()) {
    // exclude p=()-[e]->() return p limit 10
    if (yieldColumn->expr()->kind() == nebula::Expression::Kind::kPathBuild) {
      return false;
    }
  }
  return true;
}

StatusOr<OptRule::TransformResult> GetEdgesTransformRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto projectGroupNode = matched.node;
  auto project = static_cast<const Project *>(projectGroupNode->node());

  auto newProject = project->clone();
  newProject->setOutputVar(project->outputVar());
  auto newProjectGroupNode = OptGroupNode::create(ctx, newProject, projectGroupNode->group());

  auto limitGroupNode = matched.dependencies.front().node;
  auto limit = static_cast<const Limit *>(limitGroupNode->node());
  auto traverseGroupNode = matched.dependencies.front().dependencies.front().node;
  auto traverse = static_cast<const Traverse *>(traverseGroupNode->node());
  auto scanVerticesGroupNode =
      matched.dependencies.front().dependencies.front().dependencies.front().node;
  auto qctx = ctx->qctx();

  auto newLimit = limit->clone();
  auto newLimitGroup = OptGroup::create(ctx);

  auto newLimitGroupNode = newLimitGroup->makeGroupNode(newLimit);

  newProjectGroupNode->dependsOn(newLimitGroup);
  newProject->setInputVar(newLimit->outputVar());

  auto *newScanEdges =
      GetEdgesTransformUtils::traverseToScanEdges(traverse, limit->offset() + limit->count(qctx));
  if (newScanEdges == nullptr) {
    return TransformResult::noTransform();
  }
  auto newScanEdgesGroup = OptGroup::create(ctx);
  auto newScanEdgesGroupNode = newScanEdgesGroup->makeGroupNode(newScanEdges);

  auto *newProj =
      GetEdgesTransformUtils::projectEdges(qctx, newScanEdges, traverse->colNames().back());
  newProj->setColNames({traverse->colNames().back()});
  auto newProjGroup = OptGroup::create(ctx);
  auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

  newLimitGroupNode->dependsOn(newProjGroup);
  newLimit->setInputVar(newProj->outputVar());
  newLimit->setColNames(newProj->colNames());  // Limit keep column names same with input
  newProjGroupNode->dependsOn(newScanEdgesGroup);
  newProj->setInputVar(newScanEdges->outputVar());
  newScanEdgesGroupNode->setDeps(scanVerticesGroupNode->dependencies());

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newProjectGroupNode);
  return result;
}

std::string GetEdgesTransformRule::toString() const {
  return "GetEdgesTransformRule";
}

}  // namespace opt
}  // namespace nebula
