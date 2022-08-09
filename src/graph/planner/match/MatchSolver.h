/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCHSOLVER_H_
#define GRAPH_PLANNER_MATCHSOLVER_H_

#include "common/expression/ContainerExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/LabelExpression.h"
#include "graph/context/QueryContext.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"

namespace nebula {
namespace graph {

struct AstContext;

class MatchSolver final {
 public:
  MatchSolver() = delete;
  ~MatchSolver() = delete;

  static Expression* rewriteLabel2Vertex(QueryContext* qctx, const Expression* expr);

  static Expression* rewriteLabel2Edge(QueryContext* qctx, const Expression* expr);

  static Expression* rewriteLabel2VarProp(QueryContext* qctx, const Expression* expr);

  static Expression* doRewrite(QueryContext* qctx,
                               const std::unordered_map<std::string, AliasType>& aliases,
                               const Expression* expr);

  static Expression* makeIndexFilter(const std::string& label,
                                     const MapExpression* map,
                                     QueryContext* qctx,
                                     bool isEdgeProperties = false);

  static Expression* makeIndexFilter(const std::string& label,
                                     const std::string& alias,
                                     Expression* filter,
                                     QueryContext* qctx,
                                     bool isEdgeProperties = false);

  static PlanNode* filtPathHasSameEdge(PlanNode* input,
                                       const std::string& column,
                                       QueryContext* qctx);

  // Build yield columns for match & shortestPath statement
  static void buildProjectColumns(QueryContext* qctx, Path& path, SubPlan& plan);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCHSOLVER_H_
