/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_STARTVIDFINDER_H_
#define PLANNER_MATCH_STARTVIDFINDER_H_

#include "context/ast/CypherAstContext.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
class StartVidFinder;

using StartVidFinderInstantiateFunc = std::function<std::unique_ptr<StartVidFinder>()>;

class StartVidFinder {
public:
    virtual ~StartVidFinder() = default;

    static auto& finders() {
        static std::vector<StartVidFinderInstantiateFunc> finders;
        return finders;
    }

    bool match(PatternContext* patternCtx);

    virtual bool matchNode(NodeContext* nodeCtx) = 0;

    virtual bool matchEdge(EdgeContext* nodeCtx) = 0;

    StatusOr<SubPlan> transform(PatternContext* patternCtx);

    virtual StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) = 0;

    virtual StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) = 0;

protected:
    StartVidFinder() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_STARTVIDFINDER_H_
