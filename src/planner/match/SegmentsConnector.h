/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_SEGMENTSCONNECTOR_H_
#define PLANNER_MATCH_SEGMENTSCONNECTOR_H_

#include "context/QueryContext.h"
#include "context/ast/QueryAstContext.h"
#include "planner/PlanNode.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
/**
 * The SegmentsConnector was designed to be a util to help connecting the
 * plan segment.
 */
class SegmentsConnector final {
public:
    enum class ConnectStrategy : int8_t {
        kAddDependency,
        kInnerJoin,
        kLeftOuterJoin,
        kCartesianProduct,
        kUnion,
    };

    SegmentsConnector() = delete;

    // Analyse the relation of two segments and connect them.
    static StatusOr<SubPlan> connectSegments(CypherClauseContextBase* leftCtx,
                                             CypherClauseContextBase* rightCtx,
                                             SubPlan& left,
                                             SubPlan& right);

    static PlanNode* innerJoinSegments(QueryContext* qctx,
                                       const PlanNode* left,
                                       const PlanNode* right);

    static void addDependency(const PlanNode* left, const PlanNode* right);

    static void addInput(const PlanNode* left, const PlanNode* right, bool copyColNames = false);
};

class SegmentsConnectStrategy {
public:
    explicit SegmentsConnectStrategy(QueryContext* qctx) : qctx_(qctx) {}

    virtual ~SegmentsConnectStrategy() = default;

    virtual PlanNode* connect(const PlanNode* left, const PlanNode* right) = 0;

protected:
    QueryContext*   qctx_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_SEGMENTSCONNECTOR_H_
