/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCHSOLVER_H_
#define PLANNER_MATCHSOLVER_H_

#include "common/expression/ContainerExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/LabelExpression.h"
#include "context/QueryContext.h"
#include "context/ast/CypherAstContext.h"
#include "planner/Planner.h"

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

    static void extractAndDedupVidColumn(QueryContext* qctx,
                                         Expression** initialExpr,
                                         PlanNode* dep,
                                         const std::string& inputVar,
                                         SubPlan& plan);

    static Expression* initialExprOrEdgeDstExpr(QueryContext* qctx,
                                                Expression** initialExpr,
                                                const std::string& vidCol);

    static Expression* getEndVidInPath(QueryContext* qctx, const std::string& colName);

    static Expression* getStartVidInPath(QueryContext* qctx, const std::string& colName);

    static PlanNode* filtPathHasSameEdge(PlanNode* input,
                                         const std::string& column,
                                         QueryContext* qctx);

    static Status appendFetchVertexPlan(const Expression* nodeFilter,
                                        const SpaceInfo& space,
                                        QueryContext* qctx,
                                        Expression** initialExpr,
                                        SubPlan& plan);

    // In 0 step left expansion case, the result of initial index scan
    // will be passed as inputVar after right expansion is finished
    static Status appendFetchVertexPlan(const Expression* nodeFilter,
                                        const SpaceInfo& space,
                                        QueryContext* qctx,
                                        Expression** initialExpr,
                                        std::string inputVar,
                                        SubPlan& plan);
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCHSOLVER_H_
