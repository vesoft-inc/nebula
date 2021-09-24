/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/planner/ngql/FetchEdgesPlanner.h"
namespace nebula {
namespace graph {

std::unique_ptr<FetchEdgesPlanner::EdgeProps> FetchEdgesPlanner::buildEdgeProps() {
  auto eProps = std::make_unique<EdgeProps>();
  const auto &edgePropsMap = fetchCtx_->exprProps.edgeProps();
  for (const auto &edgeProp : edgePropsMap) {
    EdgeProp ep;
    ep.set_type(edgeProp.first);
    ep.set_props(std::vector<std::string>(edgeProp.second.begin(), edgeProp.second.end()));
    eProps->emplace_back(std::move(ep));
  }
  return eProps;
}

Expression *FetchEdgesPlanner::emptyEdgeFilter() {
  auto *pool = fetchCtx_->qctx->objPool();
  const auto &edgeName = fetchCtx_->edgeName;
  auto notEmpty = [&pool](Expression *expr) {
    return RelationalExpression::makeNE(pool, ConstantExpression::make(pool, Value::kEmpty), expr);
  };
  auto exprAnd = [&pool](Expression *left, Expression *right) {
    return LogicalExpression::makeAnd(pool, left, right);
  };

  auto *srcNotEmpty = notEmpty(EdgeSrcIdExpression::make(pool, edgeName));
  auto *dstNotEmpty = notEmpty(EdgeDstIdExpression::make(pool, edgeName));
  auto *rankNotEmpty = notEmpty(EdgeRankExpression::make(pool, edgeName));
  return exprAnd(srcNotEmpty, exprAnd(dstNotEmpty, rankNotEmpty));
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

  subPlan.root = Filter::make(qctx, getEdges, emptyEdgeFilter());

  subPlan.root = Project::make(qctx, subPlan.root, fetchCtx_->yieldExpr);
  if (fetchCtx_->distinct) {
    subPlan.root = Dedup::make(qctx, subPlan.root);
  }
  subPlan.tail = getEdges;
  return subPlan;
}

}  // namespace graph
}  // namespace nebula
