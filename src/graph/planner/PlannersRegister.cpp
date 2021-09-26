/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/planner/PlannersRegister.h"

#include "graph/planner/Planner.h"
#include "graph/planner/SequentialPlanner.h"
#include "graph/planner/match/LabelIndexSeek.h"
#include "graph/planner/match/MatchPlanner.h"
#include "graph/planner/match/PropIndexSeek.h"
#include "graph/planner/match/StartVidFinder.h"
#include "graph/planner/match/VertexIdSeek.h"
#include "graph/planner/ngql/FetchEdgesPlanner.h"
#include "graph/planner/ngql/FetchVerticesPlanner.h"
#include "graph/planner/ngql/GoPlanner.h"
#include "graph/planner/ngql/LookupPlanner.h"
#include "graph/planner/ngql/PathPlanner.h"
#include "graph/planner/ngql/SubgraphPlanner.h"

namespace nebula {
namespace graph {

void PlannersRegister::registPlanners() {
  registSequential();
  registMatch();
}

void PlannersRegister::registSequential() {
  {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kSequential];
    planners.emplace_back(&SequentialPlanner::match, &SequentialPlanner::make);
  }
  {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kFindPath];
    planners.emplace_back(&PathPlanner::match, &PathPlanner::make);
  }
  {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kGo];
    planners.emplace_back(&GoPlanner::match, &GoPlanner::make);
  }
  {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kLookup];
    planners.emplace_back(&LookupPlanner::match, &LookupPlanner::make);
  }
  {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kGetSubgraph];
    planners.emplace_back(&SubgraphPlanner::match, &SubgraphPlanner::make);
  }
  {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kFetchVertices];
    planners.emplace_back(&FetchVerticesPlanner::match, &FetchVerticesPlanner::make);
  }
  {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kFetchEdges];
    planners.emplace_back(&FetchEdgesPlanner::match, &FetchEdgesPlanner::make);
  }
}

void PlannersRegister::registMatch() {
  auto& planners = Planner::plannersMap()[Sentence::Kind::kMatch];

  planners.emplace_back(&MatchPlanner::match, &MatchPlanner::make);

  auto& startVidFinders = StartVidFinder::finders();

  // MATCH(n) WHERE id(n) = value RETURN n
  startVidFinders.emplace_back(&VertexIdSeek::make);

  // MATCH(n:Tag{prop:value}) RETURN n
  // MATCH(n:Tag) WHERE n.prop = value RETURN n
  startVidFinders.emplace_back(&PropIndexSeek::make);

  // seek by tag or edge(index)
  // MATCH(n: tag) RETURN n
  // MATCH(s)-[:edge]->(e) RETURN e
  startVidFinders.emplace_back(&LabelIndexSeek::make);
}

}  // namespace graph
}  // namespace nebula
