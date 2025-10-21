/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/PaginationPlanner.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> PaginationPlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kPagination) {
    return Status::Error("Not a valid context for PaginationPlanner.");
  }
  auto* paginationCtx = static_cast<PaginationContext*>(clauseCtx);

  SubPlan paginationPlan;
  NG_RETURN_IF_ERROR(buildLimit(paginationCtx, paginationPlan));
  return paginationPlan;
}

Status PaginationPlanner::buildLimit(PaginationContext* pctx, SubPlan& subplan) {
  auto* currentRoot = subplan.root;

  // Create ApproximateLimit or regular Limit based on context
  PlanNode* limit = nullptr;
  if (pctx->isApproximateLimit) {
    limit = ApproximateLimit::make(pctx->qctx,
                                   currentRoot,
                                   pctx->skip,
                                   pctx->limit,
                                   pctx->annIndexType,
                                   pctx->metricType,
                                   pctx->searchParam);
  } else {
    limit = Limit::make(pctx->qctx, currentRoot, pctx->skip, pctx->limit);
  }

  subplan.root = limit;
  subplan.tail = limit;
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
