/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/TagAnnIndexScanRule.h"

#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/OptimizerUtils.h"
#include "graph/visitor/RewriteVisitor.h"

using nebula::graph::AnnIndexScan;
using nebula::graph::ApproximateLimit;
using nebula::graph::GetVertices;
using nebula::graph::Limit;
using nebula::graph::OptimizerUtils;
using nebula::graph::ScanVertices;
using nebula::graph::Sort;
using nebula::storage::cpp2::IndexQueryContext;

namespace nebula {
namespace opt {
StatusOr<OptRule::TransformResult> runTransform(OptContext* ctx,
                                                const MatchedResult& matched,
                                                bool hasProject) {
  auto limitGroupNode = matched.node;
  auto sortGroupNode = matched.dependencies.front().node;
  auto projectGroupNode =
      hasProject ? matched.dependencies.front().dependencies.front().node : nullptr;
  auto appendVerticesGroupNode =
      hasProject ? matched.dependencies.front().dependencies.front().dependencies.front().node
                 : matched.dependencies.front().dependencies.front().node;
  auto scanVerticesGroupNode =
      hasProject ? matched.dependencies.front()
                       .dependencies.front()
                       .dependencies.front()
                       .dependencies.front()
                       .node
                 : matched.dependencies.front().dependencies.front().dependencies.front().node;

  // Get the start node from the scanVertices dependencies
  OptGroup* startGroup = nullptr;
  if (!scanVerticesGroupNode->dependencies().empty()) {
    startGroup = scanVerticesGroupNode->dependencies().front();
    LOG(ERROR) << "Start group has nodes: " << startGroup->groupNodes().size();
  } else {
    LOG(ERROR) << "No dependencies found for scanVertices node";
  }

  auto limit = static_cast<const ApproximateLimit*>(limitGroupNode->node());
  auto sort = static_cast<const Sort*>(sortGroupNode->node());
  auto appendVertices = static_cast<const GetVertices*>(appendVerticesGroupNode->node());
  auto scanVertices = static_cast<const ScanVertices*>(scanVerticesGroupNode->node());

  auto qctx = ctx->qctx();
  // Sort node information
  if (!sort->hasVectorDis()) {
    return OptRule::TransformResult::noTransform();
  }
  auto spaceId = ctx->qctx()->rctx()->session()->space().id;
  auto* distanceExpr = sort->vectorFunc();
  auto* rewritten = graph::ExpressionUtils::rewriteParameter(distanceExpr, qctx);
  auto* funcExpr = static_cast<const FunctionCallExpression*>(rewritten);
  if (!funcExpr) {
    LOG(ERROR) << "Distance function is not a FunctionCallExpression: " << distanceExpr->toString();
    return Status::Error("Distance function is not a FunctionCallExpression");
  }
  const std::string& funcName = funcExpr->name();
  static const std::unordered_set<std::string> vectorDistanceFuncs = {"euclidean", "inner_product"};
  if (vectorDistanceFuncs.find(funcName) == vectorDistanceFuncs.end()) {
    return Status::Error("Unsupported vector distance function: " + funcName);
  }
  const auto& args = funcExpr->args()->args();
  if (args.size() != 2) {
    return Status::Error("Vector distance function requires exactly 2 arguments");
  }
  std::string propertyName;
  Value queryVector;

  for (const auto& arg : args) {
    if (arg->kind() == Expression::Kind::kLabelAttribute) {
      // v.vec
      auto* labelAttrExpr = static_cast<const LabelAttributeExpression*>(arg);
      auto* constExpr = static_cast<const ConstantExpression*>(labelAttrExpr->right());
      if (constExpr && constExpr->value().isStr()) {
        propertyName = constExpr->value().getStr();
      } else {
        return Status::Error("LabelAttrExpr expression must have constant property name");
      }
    } else if (arg->kind() == Expression::Kind::kConstant) {
      auto* constExpr = static_cast<const ConstantExpression*>(arg);
      if (constExpr && constExpr->value().isVector()) {
        queryVector = constExpr->value();
      } else {
        LOG(ERROR) << "Query vector must be a vector, but got: " << queryVector.toString();
        return Status::Error("Query vector must be a vector");
      }
    } else {
      return Status::Error("Unsupported argument type in distance function: " + arg->toString());
    }
  }
  if (queryVector.isNull() || propertyName.empty()) {
    return Status::Error("Could not identify query vector and property in distance function");
  }

  IndexQueryContext iqctx;
  auto status = OptimizerUtils::createAnnIndexQueryCtx(
      qctx, scanVertices, propertyName, limit->annIndexType(), limit->metricType(), iqctx);
  if (!status.ok()) {
    return status;
  }
  // return columns
  std::vector<std::string> returnCols{kVid, kDis};
  // isEdge
  bool isEdge = false;
  // vector schema ids
  auto* vertexProps = scanVertices->props();
  std::vector<TagID> tagIds;
  for (const auto& vp : *vertexProps) {
    tagIds.emplace_back(vp.get_tag());
  }
  graph::PlanNode* startPlanNode = nullptr;
  if (startGroup && !startGroup->groupNodes().empty()) {
    startPlanNode = startGroup->groupNodes().front()->node();
  } else {
    // If no dependencies, use nullptr which should be handled by AnnIndexScan
    startPlanNode = nullptr;
  }
  auto limitNum = limit->count(qctx);
  auto queryParam = limit->param() > limitNum ? limit->param() : DEFAULT_SEARCH;
  auto newAnnIndexScan = AnnIndexScan::make(qctx,
                                            startPlanNode,
                                            spaceId,
                                            {},
                                            std::move(iqctx),
                                            std::move(returnCols),
                                            isEdge,
                                            -1,
                                            std::move(tagIds),
                                            scanVertices->dedup(),
                                            {},
                                            limitNum,
                                            nullptr,
                                            std::move(queryVector),
                                            queryParam);

  // Create a new group for the ANN index scan node
  auto newScanGroup = OptGroup::create(ctx);
  auto newScanGroupNode = newScanGroup->makeGroupNode(newAnnIndexScan);

  // Maintain original dependencies of scanVertices for the new scan node
  for (auto dep : scanVerticesGroupNode->dependencies()) {
    newScanGroupNode->dependsOn(dep);
  }

  // Create new AppendVertices node that depends on AnnIndexScan
  auto newAppendVertices = static_cast<GetVertices*>(appendVertices->clone());
  // Ensure previous input columns (e.g., _vid, _dis) are kept along with appended vertex data
  // so that distance score from AnnIndexScan can flow through.
  if (newAppendVertices->kind() == graph::PlanNode::Kind::kAppendVertices) {
    static_cast<graph::AppendVertices*>(newAppendVertices)->setTrackPrevPath(true);
  }
  newAppendVertices->setInputVar(newAnnIndexScan->outputVar());
  // 显式设置 AppendVertices 的输出列名包含 _dis，使其更直观
  newAppendVertices->setColNames({std::string("v"), kDis});
  auto newAppendVerticesGroup = OptGroup::create(ctx);
  auto newAppendVerticesGroupNode = newAppendVerticesGroup->makeGroupNode(newAppendVertices);
  newAppendVerticesGroupNode->dependsOn(newScanGroup);

  // Keep the original Project node but change its dependencies
  opt::OptGroupNode* newLimitGroupNode = nullptr;
  if (hasProject) {
    auto project = static_cast<const graph::Project*>(projectGroupNode->node());
    auto newProject = static_cast<graph::Project*>(project->clone());
    newProject->setInputVar(newAppendVertices->outputVar());
    // Rewrite any vector distance function in Project yields to pass through input `_dis`
    if (auto* yields = const_cast<YieldColumns*>(newProject->columns())) {
      for (auto* col : yields->columns()) {
        if (col == nullptr || col->expr() == nullptr) continue;
        auto* expr = col->expr();
        if (expr->kind() == Expression::Kind::kFunctionCall) {
          auto* f = static_cast<const FunctionCallExpression*>(expr);
          std::string fname = f->name();
          std::transform(fname.begin(), fname.end(), fname.begin(), ::tolower);
          if (fname == "euclidean" || fname == "inner_product") {
            auto* ip = InputPropertyExpression::make(qctx->objPool(), kDis);
            col->setExpr(ip);
            if (col->alias().empty()) {
              col->setAlias(kDis);
            }
          }
        }
      }
    }
    auto newProjectGroup = OptGroup::create(ctx);
    auto newProjectGroupNode = newProjectGroup->makeGroupNode(newProject);
    newProjectGroupNode->dependsOn(newAppendVerticesGroup);
    // Create new Limit node (replacing ApproximateLimit) that depends on Project
    auto newLimit = Limit::make(qctx, newProject, limit->offset(), limit->count(qctx));
    newLimit->setOutputVar(limit->outputVar());
    newLimitGroupNode = OptGroupNode::create(ctx, newLimit, limitGroupNode->group());
    newLimitGroupNode->dependsOn(newProjectGroup);
  } else {
    // return clause has no vector distance
    auto* pool = qctx->objPool();
    auto* yields = pool->makeAndAdd<YieldColumns>();
    // $-.v AS v
    auto* vExpr = InputPropertyExpression::make(pool, "v");
    yields->addColumn(new YieldColumn(vExpr, "v"));

    auto* passthroughProject = graph::Project::make(qctx, newAppendVertices, yields);
    passthroughProject->setInputVar(newAppendVertices->outputVar());
    // Optional: set output column names
    passthroughProject->setColNames({std::string("v")});

    auto newProjectGroup = OptGroup::create(ctx);
    auto newProjectGroupNode = newProjectGroup->makeGroupNode(passthroughProject);
    newProjectGroupNode->dependsOn(newAppendVerticesGroup);

    // Create new Limit node (replacing ApproximateLimit) that depends on Project
    auto newLimit = Limit::make(qctx, passthroughProject, limit->offset(), limit->count(qctx));
    newLimit->setOutputVar(limit->outputVar());
    newLimitGroupNode = OptGroupNode::create(ctx, newLimit, limitGroupNode->group());
    newLimitGroupNode->dependsOn(newProjectGroup);
  }

  OptRule::TransformResult result;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  result.eraseAll = true;
  return result;
}

std::unique_ptr<OptRule> TagAnnIndexScanRuleWithProject::kInstance =
    std::unique_ptr<TagAnnIndexScanRuleWithProject>(new TagAnnIndexScanRuleWithProject());

TagAnnIndexScanRuleWithProject::TagAnnIndexScanRuleWithProject() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& TagAnnIndexScanRuleWithProject::pattern() const {
  // Match pattern: ApproximateLimit -> Sort -> Project -> AppendVertices -> ScanVertices
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kApproximateLimit,
      {Pattern::create(
          graph::PlanNode::Kind::kSort,
          {Pattern::create(
              graph::PlanNode::Kind::kProject,
              {Pattern::create(graph::PlanNode::Kind::kAppendVertices,
                               {Pattern::create(graph::PlanNode::Kind::kScanVertices)})})})});
  return pattern;
}

StatusOr<OptRule::TransformResult> TagAnnIndexScanRuleWithProject::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  return runTransform(ctx, matched, true);
}

std::string TagAnnIndexScanRuleWithProject::toString() const {
  return "TagAnnIndexScanRuleWithProject";
}

std::unique_ptr<OptRule> TagAnnIndexScanRuleNoProject::kInstance =
    std::unique_ptr<TagAnnIndexScanRuleNoProject>(new TagAnnIndexScanRuleNoProject());

TagAnnIndexScanRuleNoProject::TagAnnIndexScanRuleNoProject() {
  RuleSet::DefaultRules().addRule(this);
}

const Pattern& TagAnnIndexScanRuleNoProject::pattern() const {
  // Match pattern: ApproximateLimit -> Sort -> Project -> AppendVertices -> ScanVertices
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kApproximateLimit,
      {Pattern::create(
          graph::PlanNode::Kind::kSort,
          {Pattern::create(graph::PlanNode::Kind::kAppendVertices,
                           {Pattern::create(graph::PlanNode::Kind::kScanVertices)})})});
  return pattern;
}

StatusOr<OptRule::TransformResult> TagAnnIndexScanRuleNoProject::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  return runTransform(ctx, matched, false);
}

std::string TagAnnIndexScanRuleNoProject::toString() const {
  return "TagAnnIndexScanRuleNoProject";
}

}  // namespace opt
}  // namespace nebula
