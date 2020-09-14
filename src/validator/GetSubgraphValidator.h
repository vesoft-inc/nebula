/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GETSUBGRAPHVALIDATOR_H_
#define VALIDATOR_GETSUBGRAPHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"
#include "parser/Clauses.h"

namespace nebula {
namespace graph {
class GetSubgraphValidator final : public TraversalValidator {
public:
    GetSubgraphValidator(Sentence* sentence, QueryContext* context)
        : TraversalValidator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateInBound(InBoundClause* in);

    Status validateOutBound(OutBoundClause* out);

    Status validateBothInOutBound(BothInOutClause* out);

    Expression* buildFilterCondition(int64_t step);

    GetNeighbors::EdgeProps buildEdgeProps();

    Status zeroStep(PlanNode* depend, const std::string& inputVar);

private:
    std::unordered_set<EdgeType>                edgeTypes_;
    std::string                                 collectVar_;
    std::string                                 runtimeStartVar_;
};
}  // namespace graph
}  // namespace nebula
#endif
