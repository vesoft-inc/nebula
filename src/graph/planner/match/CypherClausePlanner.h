/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_CYPHERCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_CYPHERCLAUSEPLANNER_H_

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"

namespace nebula {
namespace graph {
class CypherClausePlanner {
 public:
  CypherClausePlanner() = default;
  virtual ~CypherClausePlanner() = default;

  virtual StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) = 0;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_CYPHERCLAUSEPLANNER_H_
