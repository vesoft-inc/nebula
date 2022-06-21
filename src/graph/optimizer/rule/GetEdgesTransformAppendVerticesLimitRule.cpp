/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/GetEdgesTransformAppendVerticesLimitRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/rule/GetEdgesTransformUtils.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::AppendVertices;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::ScanEdges;
using nebula::graph::ScanVertices;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> GetEdgesTransformAppendVerticesLimitRule::kInstance =
    std::unique_ptr<GetEdgesTransformAppendVerticesLimitRule>(
        new GetEdgesTransformAppendVerticesLimitRule());

GetEdgesTransformAppendVerticesLimitRule::GetEdgesTransformAppendVerticesLimitRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &GetEdgesTransformAppendVerticesLimitRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kProject,
      {Pattern::create(
          PlanNode::Kind::kLimit,
          {Pattern::create(PlanNode::Kind::kAppendVertices,
                           {Pattern::create(PlanNode::Kind::kTraverse,
                                            {Pattern::create(PlanNode::Kind::kScanVertices)})})})});
  return pattern;
}

bool GetEdgesTransformAppendVerticesLimitRule::match(OptContext *ctx,
                                                     const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto traverse = static_cast<const Traverse *>(matched.planNode({0, 0, 0, 0}));
  auto project = static_cast<const Project *>(matched.planNode({0}));
  const auto &colNames = traverse->colNames();
  auto colSize = colNames.size();
  DCHECK_GE(colSize, 2);
  if (colNames[colSize - 2][0] != '_') {  // src
    return false;
  }
  if (traverse->stepRange() != nullptr) {
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

StatusOr<OptRule::TransformResult> GetEdgesTransformAppendVerticesLimitRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto projectGroupNode = matched.node;
  auto project = static_cast<const Project *>(projectGroupNode->node());

  auto newProject = project->clone();
  auto newProjectGroupNode = OptGroupNode::create(ctx, newProject, projectGroupNode->group());
  newProject->setOutputVar(project->outputVar());

  auto limitGroupNode = matched.dependencies.front().node;
  auto limit = static_cast<const Limit *>(limitGroupNode->node());

  auto newLimit = limit->clone();
  auto newLimitGroup = OptGroup::create(ctx);
  auto newLimitGroupNode = newLimitGroup->makeGroupNode(newLimit);
  newProject->setInputVar(newLimit->outputVar());

  newProjectGroupNode->dependsOn(newLimitGroup);

  auto appendVerticesGroupNode = matched.dependencies.front().dependencies.front().node;
  auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  auto traverseGroupNode =
      matched.dependencies.front().dependencies.front().dependencies.front().node;
  auto traverse = static_cast<const Traverse *>(traverseGroupNode->node());
  auto scanVerticesGroupNode = matched.dependencies.front()
                                   .dependencies.front()
                                   .dependencies.front()
                                   .dependencies.front()
                                   .node;
  auto qctx = ctx->qctx();

  auto newAppendVertices = appendVertices->clone();
  auto newAppendVerticesGroup = OptGroup::create(ctx);
  auto colSize = appendVertices->colNames().size();
  newLimit->setInputVar(newAppendVertices->outputVar());
  newLimit->setColNames(newAppendVertices->colNames());  // Limit keep column names same with input
  newAppendVertices->setColNames(
      {appendVertices->colNames()[colSize - 2], appendVertices->colNames()[colSize - 1]});
  auto newAppendVerticesGroupNode = newAppendVerticesGroup->makeGroupNode(newAppendVertices);

  newLimitGroupNode->dependsOn(newAppendVerticesGroup);

  auto *newScanEdges = GetEdgesTransformUtils::traverseToScanEdges(traverse, limit->count(qctx));
  if (newScanEdges == nullptr) {
    return TransformResult::noTransform();
  }
  auto newScanEdgesGroup = OptGroup::create(ctx);
  auto newScanEdgesGroupNode = newScanEdgesGroup->makeGroupNode(newScanEdges);

  auto *newProj =
      GetEdgesTransformUtils::projectEdges(qctx, newScanEdges, traverse->colNames().back());
  newProj->setInputVar(newScanEdges->outputVar());
  newProj->setColNames({traverse->colNames().back()});
  auto newProjGroup = OptGroup::create(ctx);
  auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

  newAppendVerticesGroupNode->dependsOn(newProjGroup);
  newAppendVertices->setInputVar(newProj->outputVar());
  newProjGroupNode->dependsOn(newScanEdgesGroup);
  for (auto dep : scanVerticesGroupNode->dependencies()) {
    newScanEdgesGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newProjectGroupNode);
  return result;
}

std::string GetEdgesTransformAppendVerticesLimitRule::toString() const {
  return "GetEdgesTransformAppendVerticesLimitRule";
}

}  // namespace opt
}  // namespace nebula
