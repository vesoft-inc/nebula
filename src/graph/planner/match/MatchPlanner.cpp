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
  std::vector<SubPlan> queryPartPlans;
  for (auto& queryPart : cypherCtx->queryParts) {
    SubPlan queryPartPlan;
    for (auto& match : queryPart.matchs) {
      auto matchPlanStatus = genPlan(match.get());
      NG_RETURN_IF_ERROR(matchPlanStatus);
      auto matchPlan = std::move(matchPlanStatus).value();
      if (queryPartPlan.root == nullptr) {
        queryPartPlan = matchPlan;
      } else {
        // Add a start node except the first match
        // which will be added by the sequential planner
        auto start = StartNode::make(match->qctx);
        matchPlan.tail->setDep(0, start);
        matchPlan.tail = start;
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
          queryPartPlan.root =
              BiCartesianProduct::make(match->qctx, queryPartPlan.root, matchPlan.root);
        }
      }
    }

    auto boundaryPlan = genPlan(queryPart.boundary.get());
    NG_RETURN_IF_ERROR(boundaryPlan);
    SegmentsConnector::addInput(boundaryPlan.value().tail, queryPartPlan.root);
    queryPartPlan.root = boundaryPlan.value().root;
    queryPartPlans.emplace_back(std::move(queryPartPlan));
  }

  auto finalPlan = connectQueryParts(queryPartPlans);
  NG_RETURN_IF_ERROR(finalPlan);
  return std::move(finalPlan).value();
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

StatusOr<SubPlan> MatchPlanner::connectQueryParts(std::vector<SubPlan>& subplans) {
  DCHECK(!subplans.empty());
  // subplans.front().tail->setInputVar(astCtx->qctx->vctx()->anonVarGen()->getVar());
  if (subplans.size() == 1) {
    return subplans.front();
  }

  SubPlan finalPlan = subplans.front();
  for (size_t i = 1; i < subplans.size(); ++i) {
    SegmentsConnector::addInput(subplans[i].tail, finalPlan.root);
    finalPlan.root = subplans[i].root;
  }

  return finalPlan;
}
}  // namespace graph
}  // namespace nebula
