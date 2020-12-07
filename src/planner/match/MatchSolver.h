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
#include "context/ast/QueryAstContext.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {

struct AstContext;

class MatchSolver final {
public:
    MatchSolver() = delete;
    ~MatchSolver() = delete;

    // static Status buildReturn(MatchAstContext* matchCtx, SubPlan& subPlan);

    static Expression* rewrite(const LabelExpression* label);

    static Expression* rewrite(const LabelAttributeExpression* la);

    static Expression* doRewrite(const std::unordered_map<std::string, AliasType>& aliases,
                                 const Expression* expr);

    static Expression* makeIndexFilter(const std::string& label,
                                       const MapExpression* map,
                                       QueryContext* qctx);

    static Expression* makeIndexFilter(const std::string& label,
                                       const std::string& alias,
                                       Expression* filter,
                                       QueryContext* qctx);

    static Status buildFilter(const MatchClauseContext* mctx, SubPlan* plan);

    static void extractAndDedupVidColumn(QueryContext* qctx,
                                         Expression** initialExpr,
                                         SubPlan* plan);

    static Expression* initialExprOrEdgeDstExpr(const PlanNode* node, Expression** initialExpr);

    static Expression* getLastEdgeDstExprInLastPath(const std::string& colName);

    static Expression* getFirstVertexVidInFistPath(const std::string& colName);

    static PlanNode* filtPathHasSameEdge(PlanNode* input,
                                         const std::string& column,
                                         QueryContext* qctx);
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCHSOLVER_H_
