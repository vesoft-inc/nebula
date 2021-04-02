/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_FINDPATHVALIDATOR_H_
#define VALIDATOR_FINDPATHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"

namespace nebula {
namespace graph {

class FindPathValidator final : public TraversalValidator {
public:
    FindPathValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;
    void buildEdgeProps(GetNeighbors::EdgeProps& edgeProps, bool reverse, bool isInEdge);
    void buildStart(Starts& starts, std::string& startVidsVar, bool reverse);
    GetNeighbors::EdgeProps buildEdgeKey(bool reverse);
    void linkLoopDepFromTo(PlanNode*& projectDep);
    // bfs
    Status singlePairPlan();
    PlanNode* bfs(PlanNode* dep, Starts& starts, bool reverse);
    Expression* buildBfsLoopCondition(uint32_t steps, const std::string& pathVar);

    // allPath
    Status allPairPaths();
    PlanNode* allPaths(PlanNode* dep, Starts& starts, std::string& startVidsVar, bool reverse);
    Expression* buildAllPathsLoopCondition(uint32_t steps);
    PlanNode* buildAllPairFirstDataSet(PlanNode* dep, const std::string& inputVar);

    // multi-pair
    Status multiPairPlan();
    PlanNode* multiPairShortestPath(PlanNode* dep,
                                    Starts& starts,
                                    std::string& startVidsVar,
                                    std::string& pathVar,
                                    bool reverse);
    Expression* buildMultiPairLoopCondition(uint32_t steps, std::string conditionalVar);
    PlanNode* buildMultiPairFirstDataSet(PlanNode* dep,
                                         const std::string& inputVar,
                                         const std::string& outputVar);

private:
    bool isShortest_{false};
    bool isWeight_{false};
    bool noLoop_{false};
    Starts to_;
    Over over_;
    Steps steps_;

    // runtime
    PlanNode* loopDepTail_{nullptr};
    PlanNode* toProjectStartVid_{nullptr};
    PlanNode* fromDedupStartVid_{nullptr};
    PlanNode* toDedupStartVid_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif
