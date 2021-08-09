/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {
bool StartVidFinder::match(PatternContext* patternCtx) {
    if (patternCtx->kind == PatternKind::kNode) {
        auto* nodeCtx = static_cast<NodeContext*>(patternCtx);
        return matchNode(nodeCtx);
    }

    if (patternCtx->kind == PatternKind::kEdge) {
        auto* edgeCtx = static_cast<EdgeContext*>(patternCtx);
        return matchEdge(edgeCtx);
    }

    return false;
}

StatusOr<SubPlan> StartVidFinder::transform(PatternContext* patternCtx) {
    if (patternCtx->kind == PatternKind::kNode) {
        auto* nodeCtx = static_cast<NodeContext*>(patternCtx);
        return transformNode(nodeCtx);
    }

    if (patternCtx->kind == PatternKind::kEdge) {
        auto* edgeCtx = static_cast<EdgeContext*>(patternCtx);
        return transformEdge(edgeCtx);
    }

    return Status::Error("Unknown pattern kind.");
}

}  // namespace graph
}  // namespace nebula
