/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/GetEdgesTransformRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::AppendVertices;
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
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kAppendVertices,
                      {Pattern::create(PlanNode::Kind::kTraverse,
                                       {Pattern::create(PlanNode::Kind::kScanVertices)})});
  return pattern;
}

bool GetEdgesTransformRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto traverse = static_cast<const Traverse *>(matched.planNode({0, 0}));
  const auto &colNames = traverse->colNames();
  auto colSize = colNames.size();
  DCHECK_GE(colSize, 2);
  // TODO: Poor readability for optimizer, is there any other way to do the same thing?
  if (colNames[colSize - 2][0] != '_') {  // src
    return false;
  }
  if (traverse->stepRange() != nullptr) {
    return false;
  }
  return true;
}

StatusOr<OptRule::TransformResult> GetEdgesTransformRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto appendVerticesGroupNode = matched.node;
  auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  auto traverseGroupNode = matched.dependencies.front().node;
  auto traverse = static_cast<const Traverse *>(traverseGroupNode->node());
  auto scanVerticesGroupNode = matched.dependencies.front().dependencies.front().node;
  auto qctx = ctx->qctx();

  auto newAppendVertices = appendVertices->clone();
  auto colSize = appendVertices->colNames().size();
  newAppendVertices->setColNames(
      {appendVertices->colNames()[colSize - 2], appendVertices->colNames()[colSize - 1]});
  auto newAppendVerticesGroupNode =
      OptGroupNode::create(ctx, newAppendVertices, appendVerticesGroupNode->group());

  auto *newScanEdges = traverseToScanEdges(traverse);
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

  newAppendVerticesGroupNode->dependsOn(newProjGroup);
  newProjGroupNode->dependsOn(newScanEdgesGroup);
  for (auto dep : scanVerticesGroupNode->dependencies()) {
    newScanEdgesGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newAppendVerticesGroupNode);
  return result;
}

std::string GetEdgesTransformRule::toString() const {
  return "GetEdgesTransformRule";
}

/*static*/ graph::ScanEdges *GetEdgesTransformRule::traverseToScanEdges(
    const graph::Traverse *traverse) {
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
      traverse->limit(),
      {},
      traverse->filter() == nullptr ? nullptr : traverse->filter()->clone());
  return scanEdges;
}

/*static*/ graph::Project *GetEdgesTransformRule::projectEdges(graph::QueryContext *qctx,
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
