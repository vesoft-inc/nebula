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
    SubPlan queryPartPlan;
    for (auto& match : queryPart.matchs) {
      auto matchPlanStatus = genPlan(match.get());
      NG_RETURN_IF_ERROR(matchPlanStatus);
      auto matchPlan = std::move(matchPlanStatus).value();
      connectMatch(match.get(), matchPlan, queryPartPlan);
    }

    NG_RETURN_IF_ERROR(connectQueryParts(queryPart, queryPartPlan, cypherCtx->qctx, queryPlan));
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

void MatchPlanner::connectMatch(const MatchClauseContext* match,
                                const SubPlan& matchPlan,
                                SubPlan& queryPartPlan) {
  if (queryPartPlan.root == nullptr) {
    queryPartPlan = matchPlan;
    return;
  }
  std::unordered_set<std::string> intersectedAliases;
  for (auto& alias : match->aliasesGenerated) {
    if (match->aliasesAvailable.find(alias.first) != match->aliasesAvailable.end()) {
      intersectedAliases.emplace(alias.first);
    }
  }

  if (!intersectedAliases.empty()) {
    std::vector<Expression*> hashKeys;
    std::vector<Expression*> probeKeys;
    auto pool = match->qctx->objPool();
    for (auto& alias : intersectedAliases) {
      auto* args = ArgumentList::make(pool);
      args->addArgument(InputPropertyExpression::make(pool, alias));
      auto* expr = FunctionCallExpression::make(pool, "id", args);
      hashKeys.emplace_back(expr);
      probeKeys.emplace_back(expr->clone());
    }
    if (match->isOptional) {
      auto leftJoin = BiLeftJoin::make(match->qctx, queryPartPlan.root, matchPlan.root);
      leftJoin->setHashKeys(std::move(hashKeys));
      leftJoin->setProbeKeys(std::move(probeKeys));
      queryPartPlan.root = leftJoin;
    } else {
      auto innerJoin = BiInnerJoin::make(match->qctx, queryPartPlan.root, matchPlan.root);
      innerJoin->setHashKeys(std::move(hashKeys));
      innerJoin->setProbeKeys(std::move(probeKeys));
      queryPartPlan.root = innerJoin;
    }
  } else {
    queryPartPlan.root = BiCartesianProduct::make(match->qctx, queryPartPlan.root, matchPlan.root);
  }
}

Status MatchPlanner::connectQueryParts(const QueryPart& queryPart,
                                       const SubPlan& partPlan,
                                       QueryContext* qctx,
                                       SubPlan& queryPlan) {
  auto boundaryPlan = genPlan(queryPart.boundary.get());
  NG_RETURN_IF_ERROR(boundaryPlan);
  // If this is the first query part, there will be no CartesianProduct or Join
  if (queryPlan.root == nullptr) {
    auto subplan = std::move(boundaryPlan).value();
    if (partPlan.root != nullptr) {
      SegmentsConnector::addInput(subplan.tail, partPlan.root);
      subplan.tail = partPlan.tail;
    }
    queryPlan = subplan;
    return Status::OK();
  }

  // Otherwise, there might only a with/unwind/return in a query part
  if (partPlan.root == nullptr) {
    auto subplan = std::move(boundaryPlan).value();
    SegmentsConnector::addInput(subplan.tail, queryPlan.root);
    subplan.tail = queryPlan.tail;
    queryPlan = subplan;
    return Status::OK();
  }

  auto& aliasesAvailable = queryPart.aliasesAvailable;
  std::unordered_set<std::string> intersectedAliases;
  for (auto& alias : queryPart.aliasesGenerated) {
    if (aliasesAvailable.find(alias.first) != aliasesAvailable.end()) {
      intersectedAliases.insert(alias.first);
    }
  }

  if (!intersectedAliases.empty()) {
    std::vector<Expression*> hashKeys;
    std::vector<Expression*> probeKeys;
    auto pool = qctx->objPool();
    for (auto& alias : intersectedAliases) {
      auto* args = ArgumentList::make(pool);
      args->addArgument(InputPropertyExpression::make(pool, alias));
      auto* expr = FunctionCallExpression::make(pool, "id", args);
      hashKeys.emplace_back(expr);
      probeKeys.emplace_back(expr->clone());
    }
    auto innerJoin = BiInnerJoin::make(qctx, queryPlan.root, partPlan.root);
    innerJoin->setHashKeys(std::move(hashKeys));
    innerJoin->setProbeKeys(std::move(probeKeys));
    queryPlan.root = innerJoin;
  } else {
    queryPlan.root = BiCartesianProduct::make(qctx, queryPlan.root, partPlan.root);
  }

  SegmentsConnector::addInput(boundaryPlan.value().tail, queryPlan.root);
  queryPlan.root = boundaryPlan.value().root;

  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
