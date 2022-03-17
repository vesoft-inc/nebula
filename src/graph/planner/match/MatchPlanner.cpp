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
    NG_RETURN_IF_ERROR(collectPathList(cypherCtx->qctx, queryPart.matchs, queryPartPlan));

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
    if (match->isOptional) {
      queryPartPlan =
          SegmentsConnector::leftJoin(match->qctx, queryPartPlan, matchPlan, intersectedAliases);
    } else {
      queryPartPlan =
          SegmentsConnector::innerJoin(match->qctx, queryPartPlan, matchPlan, intersectedAliases);
    }
  } else {
    queryPartPlan = SegmentsConnector::cartesianProduct(match->qctx, queryPartPlan, matchPlan);
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
      subplan = SegmentsConnector::addInput(subplan, partPlan);
    }
    queryPlan = subplan;
    if (queryPlan.tail->isSingleInput()) {
      queryPlan.tail->setInputVar(qctx->vctx()->anonVarGen()->getVar());
    }
    return Status::OK();
  }

  // Otherwise, there might only a with/unwind/return in a query part
  if (partPlan.root == nullptr) {
    auto subplan = std::move(boundaryPlan).value();
    queryPlan = SegmentsConnector::addInput(subplan, queryPlan);
    return Status::OK();
  }

  auto& aliasesAvailable = queryPart.aliasesAvailable;
  std::unordered_set<std::string> intersectedAliases;
  auto& firstMatch = queryPart.matchs.front();
  for (auto& alias : firstMatch->aliasesGenerated) {
    if (aliasesAvailable.find(alias.first) != aliasesAvailable.end()) {
      intersectedAliases.insert(alias.first);
    }
  }

  if (!intersectedAliases.empty()) {
    if (firstMatch->isOptional) {
      queryPlan =
          SegmentsConnector::leftJoin(firstMatch->qctx, queryPlan, partPlan, intersectedAliases);
    } else {
      queryPlan =
          SegmentsConnector::innerJoin(firstMatch->qctx, queryPlan, partPlan, intersectedAliases);
    }
  } else {
    queryPlan.root = BiCartesianProduct::make(qctx, queryPlan.root, partPlan.root);
  }

  queryPlan = SegmentsConnector::addInput(boundaryPlan.value(), queryPlan);
  return Status::OK();
}

/*static*/ Status MatchPlanner::collectPathList(
    QueryContext* qctx,
    const std::vector<std::unique_ptr<MatchClauseContext>>& matchs,
    SubPlan& subplan) {
  std::vector<Expression*> newGroupKeys;
  std::vector<std::string> colNames;
  std::vector<Expression*> newGroupItems;
  bool doesAgg{false};
  for (const auto& match : matchs) {
    for (const auto& pathInfo : match->paths) {
      if (!pathInfo.agg) {
        continue;
      }
      doesAgg = true;
      newGroupKeys.reserve(newGroupKeys.size() + pathInfo.groupVariables.size());
      for (const auto& variable : pathInfo.groupVariables) {
        newGroupKeys.emplace_back(InputPropertyExpression::make(qctx->objPool(), variable));
      }
      colNames.reserve(colNames.size() + pathInfo.aggVariables.size());
      newGroupItems.reserve(newGroupItems.size() + pathInfo.aggVariables.size());
      for (std::size_t i = 0; i < pathInfo.aggVariables.size() - 1; i++) {
        newGroupItems.emplace_back(
            InputPropertyExpression::make(qctx->objPool(), pathInfo.aggVariables[i]));
        colNames.emplace_back(pathInfo.aggVariables[i]);
      }
      newGroupItems.emplace_back(AggregateExpression::make(
          qctx->objPool(),
          "collect",
          InputPropertyExpression::make(qctx->objPool(), pathInfo.aggVariables.back())));
      colNames.emplace_back(pathInfo.aggVariables.back());
    }
  }
  if (doesAgg) {
    for (const auto& colName : subplan.root->colNames()) {
      if (std::find(colNames.begin(), colNames.end(), colName) == colNames.end()) {
        newGroupItems.emplace_back(InputPropertyExpression::make(qctx->objPool(), colName));
        colNames.emplace_back(colName);
      }
    }
    auto* agg =
        Aggregate::make(qctx, subplan.root, std::move(newGroupKeys), std::move(newGroupItems));
    agg->setColNames(std::move(colNames));
    subplan.root = agg;
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
