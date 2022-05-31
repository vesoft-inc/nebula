/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/GetEdgesTransformLimitRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
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

std::unique_ptr<OptRule> GetEdgesTransformLimitRule::kInstance =
    std::unique_ptr<GetEdgesTransformLimitRule>(new GetEdgesTransformLimitRule());

GetEdgesTransformLimitRule::GetEdgesTransformLimitRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &GetEdgesTransformLimitRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kProject,
      {Pattern::create(PlanNode::Kind::kLimit,
                       {Pattern::create(PlanNode::Kind::kTraverse,
                                        {Pattern::create(PlanNode::Kind::kScanVertices)})})});
  return pattern;
}

bool GetEdgesTransformLimitRule::match(OptContext *ctx, const MatchedResult &matched) const {
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

StatusOr<OptRule::TransformResult> GetEdgesTransformLimitRule::transform(
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
  newLimit->setOutputVar(limit->outputVar());
  newProject->setInputVar(newLimit->outputVar());

  newProjectGroupNode->dependsOn(newLimitGroup);

  auto traverseGroupNode = matched.dependencies.front().dependencies.front().node;
  auto traverse = static_cast<const Traverse *>(traverseGroupNode->node());
  auto scanVerticesGroupNode =
      matched.dependencies.front().dependencies.front().dependencies.front().node;
  auto qctx = ctx->qctx();

  auto *newScanEdges = traverseToScanEdges(traverse, limit->count(qctx));
  if (newScanEdges == nullptr) {
    return TransformResult::noTransform();
  }
  auto newScanEdgesGroup = OptGroup::create(ctx);
  auto newScanEdgesGroupNode = newScanEdgesGroup->makeGroupNode(newScanEdges);

  auto *newProj = projectEdges(qctx, newScanEdges, traverse->colNames().back());
  newProj->setInputVar(newScanEdges->outputVar());
  newProj->setOutputVar(traverse->outputVar());
  newProj->setColNames({traverse->colNames().back()});
  auto newProjGroup = OptGroup::create(ctx);
  auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

  newLimit->setInputVar(newProj->outputVar());
  newLimitGroupNode->dependsOn(newProjGroup);

  newProjGroupNode->dependsOn(newScanEdgesGroup);
  newScanEdgesGroupNode->setDeps(scanVerticesGroupNode->dependencies());

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newProjectGroupNode);
  return result;
}

std::string GetEdgesTransformLimitRule::toString() const {
  return "GetEdgesTransformLimitRule";
}

/*static*/ graph::ScanEdges *GetEdgesTransformLimitRule::traverseToScanEdges(
    const graph::Traverse *traverse, const int64_t limit_count = -1) {
  const auto *edgeProps = traverse->edgeProps();
  if (edgeProps == nullptr) {
    return nullptr;
  }
  for (std::size_t i = 0; i < edgeProps->size(); i++) {
    auto type = (*edgeProps)[i].get_type();
    for (std::size_t j = i + 1; j < edgeProps->size(); j++) {
      if (type == -((*edgeProps)[j].get_type())) {
        // Don't support to retrieve edges of the inbound/outbound together
        return nullptr;
      }
    }
  }
  auto scanEdges = ScanEdges::make(
      traverse->qctx(),
      nullptr,
      traverse->space(),
      edgeProps == nullptr ? nullptr
                           : std::make_unique<std::vector<storage::cpp2::EdgeProp>>(*edgeProps),
      nullptr,
      traverse->dedup(),
      limit_count,
      {},
      traverse->filter() == nullptr ? nullptr : traverse->filter()->clone());
  return scanEdges;
}

/*static*/ graph::Project *GetEdgesTransformLimitRule::projectEdges(graph::QueryContext *qctx,
                                                                    PlanNode *input,
                                                                    const std::string &colName) {
  auto *yieldColumns = qctx->objPool()->makeAndAdd<YieldColumns>();
  auto *edgeExpr = EdgeExpression::make(qctx->objPool());
  auto *listEdgeExpr = ListExpression::make(qctx->objPool());
  listEdgeExpr->setItems({edgeExpr});
  yieldColumns->addColumn(new YieldColumn(listEdgeExpr, colName));
  auto project = Project::make(qctx, input, yieldColumns);
  project->setColNames({colName});
  return project;
}

}  // namespace opt
}  // namespace nebula
