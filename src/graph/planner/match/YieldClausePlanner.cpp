/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/YieldClausePlanner.h"

#include "graph/planner/match/MatchPathPlanner.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> YieldClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kYield) {
    return Status::Error("Not a valid context for YieldClausePlanner.");
  }
  auto* yieldCtx = static_cast<YieldClauseContext*>(clauseCtx);

  SubPlan yieldPlan;
  NG_RETURN_IF_ERROR(buildYield(yieldCtx, yieldPlan));
  return yieldPlan;
}

void YieldClausePlanner::rewriteYieldColumns(const YieldClauseContext* yctx,
                                             const YieldColumns* yields,
                                             YieldColumns* newYields) {
  for (auto* col : yields->columns()) {
    auto* expr = MatchSolver::doRewrite(yctx->qctx, yctx->aliasesAvailable, col->expr());
    newYields->addColumn(new YieldColumn(expr, col->alias()));
  }
}

void YieldClausePlanner::rewriteGroupExprs(const YieldClauseContext* yctx,
                                           const std::vector<Expression*>* exprs,
                                           std::vector<Expression*>* newExprs) {
  for (auto* expr : *exprs) {
    auto* newExpr = MatchSolver::doRewrite(yctx->qctx, yctx->aliasesAvailable, expr);
    newExprs->emplace_back(newExpr);
  }
}

Status YieldClausePlanner::buildYield(YieldClauseContext* yctx, SubPlan& subplan) {
  auto* currentRoot = subplan.root;
  DCHECK(!currentRoot);
  auto* newProjCols = yctx->qctx->objPool()->makeAndAdd<YieldColumns>();
  rewriteYieldColumns(yctx, yctx->projCols_, newProjCols);
  if (!yctx->hasAgg_) {
    auto* project = Project::make(yctx->qctx, currentRoot, newProjCols);
    project->setColNames(std::move(yctx->projOutputColumnNames_));
    subplan.root = project;
    subplan.tail = project;
  } else {
    std::vector<Expression*> newGroupKeys;
    std::vector<Expression*> newGroupItems;
    rewriteGroupExprs(yctx, &yctx->groupKeys_, &newGroupKeys);
    rewriteGroupExprs(yctx, &yctx->groupItems_, &newGroupItems);

    auto* agg =
        Aggregate::make(yctx->qctx, currentRoot, std::move(newGroupKeys), std::move(newGroupItems));
    agg->setColNames(std::vector<std::string>(yctx->aggOutputColumnNames_));
    if (yctx->needGenProject_) {
      auto* project = Project::make(yctx->qctx, agg, newProjCols);
      project->setInputVar(agg->outputVar());
      project->setColNames(std::move(yctx->projOutputColumnNames_));
      subplan.root = project;
    } else {
      subplan.root = agg;
    }
    subplan.tail = agg;
  }

  if (yctx->distinct) {
    subplan.root = Dedup::make(yctx->qctx, subplan.root);
  }
  SubPlan patternPlan;
  // Build plan for pattern from expression
  for (auto& path : yctx->paths) {
    auto status = MatchPathPlanner(yctx, path).transform(nullptr, {});
    NG_RETURN_IF_ERROR(status);
    patternPlan =
        SegmentsConnector::rollUpApply(yctx, patternPlan, std::move(status).value(), path);
  }
  if (patternPlan.root != nullptr) {
    subplan = SegmentsConnector::addInput(subplan, patternPlan);
  }

  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
