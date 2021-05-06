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

    static Expression* rewriteLabel2Vertex(const Expression* expr);

    static Expression* rewriteLabel2Edge(const Expression* expr);

    static Expression* rewriteLabel2VarProp(const Expression* expr);

    static Expression* doRewrite(const std::unordered_map<std::string, AliasType>& aliases,
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
                                         Expression* initialExpr,
                                         PlanNode* dep,
                                         const std::string& inputVar,
                                         SubPlan& plan);

    static Expression* initialExprOrEdgeDstExpr(Expression* initialExpr, const std::string& vidCol);

    static Expression* getEndVidInPath(const std::string& colName);

    static Expression* getStartVidInPath(const std::string& colName);

    static PlanNode* filtPathHasSameEdge(PlanNode* input,
                                         const std::string& column,
                                         QueryContext* qctx);

    static Status appendFetchVertexPlan(const Expression* nodeFilter,
                                        const SpaceInfo& space,
                                        QueryContext* qctx,
                                        Expression* initialExpr,
                                        SubPlan& plan);

    // In 0 step left expansion case, the result of initial index scan
    // will be passed as inputVar after right expansion is finished
    static Status appendFetchVertexPlan(const Expression* nodeFilter,
                                        const SpaceInfo& space,
                                        QueryContext* qctx,
                                        Expression* initialExpr,
                                        std::string inputVar,
                                        SubPlan& plan);

    // Fetch all tags in the space and retrieve props from tags
    static StatusOr<std::vector<storage::cpp2::VertexProp>> flattenTags(QueryContext* qctx,
                                                                        const SpaceInfo& space);
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCHSOLVER_H_
