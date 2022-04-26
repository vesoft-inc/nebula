/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/UnwindClausePlanner.h"

#include "graph/planner/match/MatchPathPlanner.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/OrderByClausePlanner.h"
#include "graph/planner/match/PaginationPlanner.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> UnwindClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kUnwind) {
    return Status::Error("Not a valid context for UnwindClausePlanner.");
  }
  auto* unwindClauseCtx = static_cast<UnwindClauseContext*>(clauseCtx);

  SubPlan unwindPlan;
  NG_RETURN_IF_ERROR(buildUnwind(unwindClauseCtx, unwindPlan));

  SubPlan subPlan;
  // Build plan for pattern from expression
  for (auto& path : unwindClauseCtx->paths) {
    auto pathPlan =
        std::make_unique<MatchPathPlanner>()->transform(unwindClauseCtx->qctx,
                                                        unwindClauseCtx->space.id,
                                                        nullptr,
                                                        unwindClauseCtx->aliasesAvailable,
                                                        {},
                                                        path);
    NG_RETURN_IF_ERROR(pathPlan);
    auto pathplan = std::move(pathPlan).value();
    subPlan = SegmentsConnector::rollUpApply(unwindClauseCtx->qctx,
                                             subPlan,
                                             unwindClauseCtx->inputColNames,
                                             pathplan,
                                             path.compareVariables,
                                             path.collectVariable);
  }

  if (subPlan.root != nullptr) {
    unwindPlan = SegmentsConnector::addInput(unwindPlan, subPlan);
  }
  return unwindPlan;
}

Status UnwindClausePlanner::buildUnwind(UnwindClauseContext* uctx, SubPlan& subPlan) {
  auto* newUnwindExpr =
      MatchSolver::doRewrite(uctx->qctx, uctx->aliasesAvailable, uctx->unwindExpr);
  auto* unwind = Unwind::make(uctx->qctx, nullptr, newUnwindExpr, uctx->alias);
  unwind->setColNames({uctx->alias});
  subPlan.root = unwind;
  subPlan.tail = unwind;

  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
