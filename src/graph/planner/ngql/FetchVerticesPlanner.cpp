/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/FetchVerticesPlanner.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/PlannerUtil.h"

namespace nebula {
namespace graph {

std::unique_ptr<FetchVerticesPlanner::VertexProps> FetchVerticesPlanner::buildVertexProps(
    const ExpressionProps::TagIDPropsMap& propsMap) {
  if (propsMap.empty()) {
    return std::make_unique<FetchVerticesPlanner::VertexProps>();
  }
  auto vertexProps = std::make_unique<VertexProps>(propsMap.size());
  auto fun = [](auto& tag) {
    VertexProp vp;
    vp.tag_ref() = tag.first;
    std::vector<std::string> props(tag.second.begin(), tag.second.end());
    vp.props_ref() = std::move(props);
    return vp;
  };
  std::transform(propsMap.begin(), propsMap.end(), vertexProps->begin(), fun);
  return vertexProps;
}

StatusOr<SubPlan> FetchVerticesPlanner::transform(AstContext* astCtx) {
  fetchCtx_ = static_cast<FetchVerticesContext*>(astCtx);
  auto qctx = fetchCtx_->qctx;
  auto space = fetchCtx_->space;
  auto& starts = fetchCtx_->from;

  std::string vidsVar;
  if (!starts.vids.empty() && starts.originalSrc == nullptr) {
    PlannerUtil::buildConstantInput(qctx, starts, vidsVar);
  } else {
    starts.src = starts.originalSrc;
    if (starts.fromType == kVariable) {
      vidsVar = starts.userDefinedVarName;
    } else {
      vidsVar = fetchCtx_->inputVarName;
    }
  }

  SubPlan subPlan;
  auto* getVertices = GetVertices::make(qctx,
                                        nullptr,
                                        space.id,
                                        starts.src,
                                        buildVertexProps(fetchCtx_->exprProps.tagProps()),
                                        {},
                                        fetchCtx_->distinct);
  getVertices->setInputVar(vidsVar);

  subPlan.root = Project::make(qctx, getVertices, fetchCtx_->yieldExpr);
  if (fetchCtx_->distinct) {
    subPlan.root = Dedup::make(qctx, subPlan.root);
  }
  subPlan.tail = getVertices;
  return subPlan;
}

}  // namespace graph
}  // namespace nebula
