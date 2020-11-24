/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlannersRegister.h"

#include "planner/Planner.h"
#include "planner/planners/MatchTagScanPlanner.h"
#include "planner/planners/MatchVertexIdSeekPlanner.h"
// #include "planner/planners/MatchVertexIndexSeekPlanner.h"
#include "planner/planners/MatchVariableLengthPatternIndexScanPlanner.h"
#include "planner/planners/SequentialPlanner.h"

namespace nebula {
namespace graph {
void PlannersRegister::registPlanners() {
    registSequential();
    registMatch();
}

void PlannersRegister::registSequential() {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kSequential];
    planners.emplace_back(&SequentialPlanner::match, &SequentialPlanner::make);
}

void PlannersRegister::registMatch() {
    auto& planners = Planner::plannersMap()[Sentence::Kind::kMatch];

    // MATCH(n) WHERE id(n) = value RETURN n
    planners.emplace_back(&MatchVertexIdSeekPlanner::match, &MatchVertexIdSeekPlanner::make);

    // MATCH(n:Tag{prop:value}) RETURN n
    // MATCH(n:Tag) WHERE n.prop = value RETURN n
    // planners.emplace_back(&MatchVertexIndexSeekPlanner::match,
    //     &MatchVertexIndexSeekPlanner::make);
    planners.emplace_back(&MatchVariableLengthPatternIndexScanPlanner::match,
                          &MatchVariableLengthPatternIndexScanPlanner::make);

    // MATCH(n:Tag) RETURN n;
    planners.emplace_back(&MatchTagScanPlanner::match, &MatchTagScanPlanner::make);
}
}  // namespace graph
}  // namespace nebula
