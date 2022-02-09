/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/WhereClausePlanner.h"

#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> WhereClausePlanner::transform(CypherClauseContextBase* ctx) {
  if (ctx->kind != CypherClauseKind::kWhere) {
    return Status::Error("Not a valid context for WhereClausePlanner.");
  }

  auto* wctx = static_cast<WhereClauseContext*>(ctx);
  if (wctx->filter) {
    SubPlan wherePlan;
    auto* newFilter = MatchSolver::doRewrite(wctx->qctx, wctx->aliasesAvailable, wctx->filter);
    wherePlan.root = Filter::make(wctx->qctx, nullptr, newFilter, needStableFilter_);
    wherePlan.tail = wherePlan.root;

    return wherePlan;
  }

  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
