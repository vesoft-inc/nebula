/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCHVERTEXIDSEEK_H_
#define PLANNER_PLANNERS_MATCHVERTEXIDSEEK_H_

#include "context/QueryContext.h"
#include "planner/Planner.h"
#include "validator/MatchValidator.h"

namespace nebula {
namespace graph {
class MatchVertexIdSeekPlanner final : public Planner {
public:
    static std::unique_ptr<MatchVertexIdSeekPlanner> make() {
        return std::unique_ptr<MatchVertexIdSeekPlanner>(new MatchVertexIdSeekPlanner());
    }

    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    static StatusOr<const Expression*> extractVids(const Expression* filter);

    MatchVertexIdSeekPlanner() = default;

    std::pair<std::string, Expression*> listToAnnoVarVid(const List& list);

    std::pair<std::string, Expression*> constToAnnoVarVid(const Value& v);

    Status buildQueryById();

    Status buildProjectVertices();

    SubPlan             subPlan_;
    MatchAstContext*    matchCtx_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCHVERTEXIDSEEK_H_
