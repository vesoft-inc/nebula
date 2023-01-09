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
  SubPlan plan;
  if (!wctx->paths.empty()) {
    SubPlan pathsPlan;
    // Build plan for pattern expression
    for (auto& path : wctx->paths) {
      auto status = MatchPathPlanner(wctx, path).transform(nullptr, {});
      NG_RETURN_IF_ERROR(status);
      auto pathPlan = std::move(status).value();

      if (path.isPred) {
        // Build plan for pattern predicates
        pathsPlan = SegmentsConnector::patternApply(wctx, pathsPlan, pathPlan, path);
      } else {
        pathsPlan = SegmentsConnector::rollUpApply(wctx, pathsPlan, pathPlan, path);
      }
    }
    plan = pathsPlan;
  }

  if (wctx->filter) {
    SubPlan wherePlan;
    auto* newFilter = MatchSolver::doRewrite(wctx->qctx, wctx->aliasesAvailable, wctx->filter);
    wherePlan.root = Filter::make(wctx->qctx, nullptr, newFilter, needStableFilter_);
    wherePlan.tail = wherePlan.root;
    if (plan.root == nullptr) {
      return wherePlan;
    }
    plan = SegmentsConnector::addInput(wherePlan, plan, true);
  }

  return plan;
}
}  // namespace graph
}  // namespace nebula
