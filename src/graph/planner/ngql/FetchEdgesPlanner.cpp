/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/planner/ngql/FetchEdgesPlanner.h"

#include "graph/util/ExpressionUtils.h"
#include "graph/util/QueryUtil.h"
#include "graph/util/SchemaUtil.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {

std::unique_ptr<FetchEdgesPlanner::EdgeProps> FetchEdgesPlanner::buildEdgeProps() {
  auto eProps = std::make_unique<EdgeProps>();
  auto edgePropsMap = fetchCtx_->exprProps.edgeProps();
  for (const auto &edgeProp : edgePropsMap) {
    EdgeProp ep;
    ep.set_type(edgeProp.first);
    ep.set_props(std::vector<std::string>(edgeProp.second.begin(), edgeProp.second.end()));
    eProps->emplace_back(std::move(ep));
  }
  return eProps;
}

StatusOr<SubPlan> FetchEdgesPlanner::transform(AstContext *astCtx) {
  fetchCtx_ = static_cast<FetchEdgesContext *>(astCtx);
  auto qctx = fetchCtx_->qctx;
  auto spaceID = fetchCtx_->space.id;

  SubPlan subPlan;
  auto *getEdges = GetEdges::make(qctx,
                                  nullptr,
                                  spaceID,
                                  fetchCtx_->src,
                                  fetchCtx_->type,
                                  fetchCtx_->rank,
                                  fetchCtx_->dst,
                                  buildEdgeProps(),
                                  {},
                                  fetchCtx_->distinct);
  getEdges->setInputVar(fetchCtx_->inputVarName);

  subPlan.root = Project::make(qctx, getEdges, fetchCtx_->yieldExpr);
  if (fetchCtx_->distinct) {
    subPlan.root = Dedup::make(qctx, subPlan.root);
  }
  subPlan.tail = getEdges;
  return subPlan;
}

}  // namespace graph
}  // namespace nebula
