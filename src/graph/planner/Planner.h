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

namespace nebula {
namespace graph {
class Planner;

extern const char* kSrcVID;
extern const char* kDstVID;
extern const char* kRanking;
extern const char* kVertexID;
extern const char* kVertices;
extern const char* kEdges;

std::ostream& operator<<(std::ostream& os, const SubPlan& subplan);

using MatchFunc = std::function<bool(AstContext* astContext)>;
using PlannerInstantiateFunc = std::function<std::unique_ptr<Planner>()>;

struct MatchAndInstantiate {
  MatchAndInstantiate(MatchFunc m, PlannerInstantiateFunc p)
      : match(std::move(m)), instantiate(std::move(p)) {}
  MatchFunc match;
  PlannerInstantiateFunc instantiate;
};

class Planner {
 public:
  virtual ~Planner() = default;

  static auto& plannersMap() {
    static std::unordered_map<Sentence::Kind, std::vector<MatchAndInstantiate>> plannersMap;
    return plannersMap;
  }

  static StatusOr<SubPlan> toPlan(AstContext* astCtx);

  virtual StatusOr<SubPlan> transform(AstContext* astCtx) = 0;

 protected:
  Planner() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLANNER_H_
