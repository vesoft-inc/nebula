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

    Status singlePairPlan();

    void buildStart(const Starts& starts,
                    std::string& startVidsVar,
                    PlanNode* dedupStartVid,
                    Expression* src);

    PlanNode* bfs(PlanNode* dep, const Starts& starts, bool reverse);

    Expression* buildBfsLoopCondition(uint32_t steps, const std::string& pathVar);

    GetNeighbors::EdgeProps buildEdgeKey(bool reverse);

private:
    bool            isShortest_{false};
    Starts          to_;
    Over            over_;
    Steps           steps_;
};
}  // namespace graph
}  // namespace nebula
#endif
