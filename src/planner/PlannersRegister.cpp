/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlannersRegister.h"

#include "planner/Planner.h"
#include "planner/SequentialPlanner.h"
#include "planner/match/MatchPlanner.h"
#include "planner/match/StartVidFinder.h"
#include "planner/match/PropIndexSeek.h"
#include "planner/match/VertexIdSeek.h"
#include "planner/match/LabelIndexSeek.h"
#include "planner/ngql/LookupPlanner.h"
#include "planner/ngql/PathPlanner.h"
#include "planner/ngql/GoPlanner.h"
#include "planner/ngql/SubgraphPlanner.h"

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
