/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/WhereClausePlanner.h"

#include "graph/planner/match/MatchPathPlanner.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> WhereClausePlanner::transform(CypherClauseContextBase* ctx) {
  if (ctx->kind != CypherClauseKind::kWhere) {
    return Status::Error("Not a valid context for WhereClausePlanner.");
  }

  auto* wctx = static_cast<WhereClauseContext*>(ctx);
  SubPlan wherePlan;
  if (wctx->filter) {
    auto* newFilter = MatchSolver::doRewrite(wctx->qctx, wctx->aliasesAvailable, wctx->filter);
    wherePlan.root = Filter::make(wctx->qctx, nullptr, newFilter, needStableFilter_);
    wherePlan.tail = wherePlan.root;

    SubPlan subPlan;
    // Build plan for pattern from expression
    for (auto& path : wctx->paths) {
      auto pathPlan = std::make_unique<MatchPathPlanner>()->transform(
          wctx->qctx, wctx->space.id, nullptr, wctx->aliasesAvailable, {}, path);
      NG_RETURN_IF_ERROR(pathPlan);
      auto pathplan = std::move(pathPlan).value();
      subPlan = SegmentsConnector::rollUpApply(wctx->qctx,
                                               subPlan,
                                               wctx->inputColNames,
                                               pathplan,
                                               path.compareVariables,
                                               path.collectVariable);
    }
    if (subPlan.root != nullptr) {
      wherePlan = SegmentsConnector::addInput(wherePlan, subPlan);
    }

    return wherePlan;
  }

  return wherePlan;
}
}  // namespace graph
}  // namespace nebula
