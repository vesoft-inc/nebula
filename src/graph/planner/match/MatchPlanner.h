/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCHPLANNER_H_
#define GRAPH_PLANNER_MATCHPLANNER_H_

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"

namespace nebula {
namespace graph {
// MatchPlanner generates plans for Match statement based on AstContext.
class MatchPlanner final : public Planner {
 public:
  static std::unique_ptr<MatchPlanner> make() {
    return std::make_unique<MatchPlanner>();
  }

  static bool match(AstContext* astCtx);

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

 private:
  StatusOr<SubPlan> genPlan(CypherClauseContextBase* clauseCtx);

  void connectMatch(const MatchClauseContext* match,
                    const SubPlan& matchPlan,
                    SubPlan& queryPartPlan);

  Status connectQueryParts(const QueryPart& queryPart,
                           const SubPlan& partPlan,
                           QueryContext* qctx,
                           SubPlan& queryPlan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLANNERS_MATCHPLANNER_H_
