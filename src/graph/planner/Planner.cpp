/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Planner.h"

#include "validator/Validator.h"

namespace nebula {
namespace graph {

const char* kSrcVID = "SrcVID";
const char* kDstVID = "DstVID";
const char* kRanking = "Ranking";
const char* kVertexID = "VertexID";
const char* kVertices = "_vertices";
const char* kEdges = "_edges";

std::ostream& operator<<(std::ostream& os, const SubPlan& subplan) {
    os << "root(" << subplan.root->toString() << "): " << subplan.root->outputVar() << ", tail("
       << subplan.tail->toString() << "): " << subplan.tail->outputVar();
    return os;
}

StatusOr<SubPlan> Planner::toPlan(AstContext* astCtx) {
    if (astCtx == nullptr) {
        return Status::Error("AstContext nullptr.");
    }
    const auto* sentence = astCtx->sentence;
    DCHECK(sentence != nullptr);
    auto planners = plannersMap().find(sentence->kind());
    if (planners == plannersMap().end()) {
        return Status::Error("No planners for sentence: %s", sentence->toString().c_str());
    }
    for (auto& planner : planners->second) {
        if (planner.match(astCtx)) {
            return planner.instantiate()->transform(astCtx);
        }
    }
    return Status::Error("No planner matches sentence: %s", sentence->toString().c_str());
}
}  // namespace graph
}  // namespace nebula
