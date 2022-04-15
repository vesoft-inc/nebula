/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/MatchPlanner.h"

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/MatchClausePlanner.h"
#include "graph/planner/match/ReturnClausePlanner.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/UnwindClausePlanner.h"
#include "graph/planner/match/WhereClausePlanner.h"
#include "graph/planner/match/WithClausePlanner.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
bool MatchPlanner::match(AstContext* astCtx) {
  return astCtx->sentence->kind() == Sentence::Kind::kMatch;
}

StatusOr<SubPlan> MatchPlanner::transform(AstContext* astCtx) {
  if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
    return Status::Error("Only MATCH is accepted for match planner.");
  }
  auto* cypherCtx = static_cast<CypherContext*>(astCtx);
  SubPlan queryPlan;
  for (auto& queryPart : cypherCtx->queryParts) {
    NG_RETURN_IF_ERROR(genQueryPartPlan(cypherCtx->qctx, queryPlan, queryPart));
  }

  return queryPlan;
}

StatusOr<SubPlan> MatchPlanner::genPlan(CypherClauseContextBase* clauseCtx) {
  switch (clauseCtx->kind) {
    case CypherClauseKind::kMatch: {
      return std::make_unique<MatchClausePlanner>()->transform(clauseCtx);
    }
    case CypherClauseKind::kUnwind: {
      return std::make_unique<UnwindClausePlanner>()->transform(clauseCtx);
    }
    case CypherClauseKind::kWith: {
      return std::make_unique<WithClausePlanner>()->transform(clauseCtx);
    }
    case CypherClauseKind::kReturn: {
      return std::make_unique<ReturnClausePlanner>()->transform(clauseCtx);
    }
    default: {
      return Status::Error("Unsupported clause.");
    }
  }
  return Status::OK();
}

// Connect current match plan to previous queryPlan
Status MatchPlanner::connectMatchPlan(SubPlan& queryPlan, MatchClauseContext* matchCtx) {
  // Generate current match plan
  auto matchPlanStatus = genPlan(matchCtx);
  NG_RETURN_IF_ERROR(matchPlanStatus);
  auto matchPlan = std::move(matchPlanStatus).value();

  if (queryPlan.root == nullptr) {
    queryPlan = matchPlan;
    return Status::OK();
  }
  // Connect to queryPlan
  const auto& path = matchCtx->paths.back();
  if (path.rollUpApply) {
    queryPlan = SegmentsConnector::rollUpApply(
        matchCtx->qctx, queryPlan, matchPlan, path.compareVariables, path.collectVariable);
    return Status::OK();
  }

  std::unordered_set<std::string> intersectedAliases;
  for (auto& alias : matchCtx->aliasesGenerated) {
    if (matchCtx->aliasesAvailable.find(alias.first) != matchCtx->aliasesAvailable.end()) {
      intersectedAliases.insert(alias.first);
    }
  }
  if (!intersectedAliases.empty()) {
    if (matchCtx->isOptional) {
      // connect LeftJoin match filter
      if (matchCtx->where != nullptr) {
        auto wherePlanStatus =
            std::make_unique<WhereClausePlanner>()->transform(matchCtx->where.get());
        NG_RETURN_IF_ERROR(wherePlanStatus);
        auto wherePlan = std::move(wherePlanStatus).value();
        matchPlan = SegmentsConnector::addInput(wherePlan, matchPlan, true);
      }
      queryPlan =
          SegmentsConnector::leftJoin(matchCtx->qctx, queryPlan, matchPlan, intersectedAliases);
    } else {
      queryPlan =
          SegmentsConnector::innerJoin(matchCtx->qctx, queryPlan, matchPlan, intersectedAliases);
    }
  } else {
    queryPlan.root = BiCartesianProduct::make(matchCtx->qctx, queryPlan.root, matchPlan.root);
  }

  return Status::OK();
}

Status MatchPlanner::genQueryPartPlan(QueryContext* qctx,
                                      SubPlan& queryPlan,
                                      const QueryPart& queryPart) {
  // generate plan for matchs
  for (auto& match : queryPart.matchs) {
    connectMatchPlan(queryPlan, match.get());
    // connect match filter
    if (match->where != nullptr && !match->isOptional) {
      auto wherePlanStatus = std::make_unique<WhereClausePlanner>()->transform(match->where.get());
      NG_RETURN_IF_ERROR(wherePlanStatus);
      auto wherePlan = std::move(wherePlanStatus).value();
      queryPlan = SegmentsConnector::addInput(wherePlan, queryPlan, true);
    }
  }

  // generate plan for boundary
  auto boundaryPlanStatus = genPlan(queryPart.boundary.get());
  NG_RETURN_IF_ERROR(boundaryPlanStatus);
  auto boundaryPlan = std::move(boundaryPlanStatus).value();
  if (queryPlan.root == nullptr) {
    queryPlan = boundaryPlan;
  } else {
    queryPlan = SegmentsConnector::addInput(boundaryPlan, queryPlan, false);
  }

  // TBD: need generate var for all queryPlan.tail?
  if (queryPlan.tail->isSingleInput()) {
    queryPlan.tail->setInputVar(qctx->vctx()->anonVarGen()->getVar());
    if (!tailConnected_) {
      auto start = StartNode::make(qctx);
      queryPlan.tail->setDep(0, start);
      tailConnected_ = true;
      queryPlan.tail = start;
    }
  }
  VLOG(1) << queryPlan;

  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
