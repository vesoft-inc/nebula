/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/IsomorPlanner.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/PlannerUtil.h"

namespace nebula {
namespace graph {

std::unique_ptr<IsomorPlanner::VertexProps> IsomorPlanner::buildVertexProps(
    const ExpressionProps::TagIDPropsMap& propsMap) {
  if (propsMap.empty()) {
    return std::make_unique<IsomorPlanner::VertexProps>();
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

StatusOr<SubPlan> IsomorPlanner::transform(AstContext* astCtx) {
  fetchCtx_ = static_cast<IsomorContext*>(astCtx);
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
  // TODO: Add some to do while combining the executor and the planner.
  //  (1) create a plan node for Isomorphism.cpp
  //  (2) Define the Input and output of the plan node function.
  //  (3) Add the Register.
  SubPlan subPlan;
  auto* getVertices = GetVertices::make(qctx,
                                        nullptr,
                                        space.id,
                                        starts.src,
                                        buildVertexProps(fetchCtx_->exprProps.tagProps()),
                                        {},
                                        fetchCtx_->distinct);
  getVertices->setInputVar(vidsVar);

  subPlan.root = Project::make(qctx, getVertices);
  if (fetchCtx_->distinct) {
    subPlan.root = Dedup::make(qctx, subPlan.root);
  }
  subPlan.tail = getVertices;
  return subPlan;
}

}  // namespace graph
}  // namespace nebula
