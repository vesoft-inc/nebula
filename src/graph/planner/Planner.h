/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLANNER_H_
#define GRAPH_PLANNER_PLANNER_H_

#include <ostream>

#include "common/base/Base.h"
#include "graph/context/ast/AstContext.h"
#include "graph/planner/plan/ExecutionPlan.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/util/Constants.h"

namespace nebula {
namespace graph {
class Planner;

std::ostream& operator<<(std::ostream& os, const SubPlan& subplan);

using MatchFunc = std::function<bool(AstContext* astContext)>;
using PlannerInstantiateFunc = std::function<std::unique_ptr<Planner>()>;

struct MatchAndInstantiate {
  MatchAndInstantiate(MatchFunc m, PlannerInstantiateFunc p)
      : match(std::move(m)), instantiate(std::move(p)) {}
  MatchFunc match;
  PlannerInstantiateFunc instantiate;
};

// A planner generates plans for statements.
// For example, we have MatchPlanner that generates plan for Match statement.
// And we have GoPlanner that generates plan for Go statements. Each planner
// will be registered into the plannersMap in the PlannersRegister when the
// graphd service starts up.
class Planner {
 public:
  virtual ~Planner() = default;

  // Each statement might have many planners that match different situations.
  // Each planner should provide two functions:
  // 1. MatchFunc that matchs the proper situation
  // 2. PlannerInstantiateFunc that instantiates the planner
  static auto& plannersMap() {
    static std::unordered_map<Sentence::Kind, std::vector<MatchAndInstantiate>> plannersMap;
    return plannersMap;
  }

  // Generates plans for each statement based on AsrContext.
  static StatusOr<SubPlan> toPlan(AstContext* astCtx);

  // Transforms the AstContext to a plan which determined by the implementations.
  virtual StatusOr<SubPlan> transform(AstContext* astCtx) = 0;

 protected:
  Planner() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLANNER_H_
