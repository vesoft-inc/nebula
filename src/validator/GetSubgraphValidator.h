/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GETSUBGRAPHVALIDATOR_H_
#define VALIDATOR_GETSUBGRAPHVALIDATOR_H_

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

    StatusOr<GetNeighbors::EdgeProps> buildEdgeProps();

    Status zeroStep(PlanNode* depend, const std::string& inputVar);

    StatusOr<std::vector<storage::cpp2::VertexProp>> buildVertexProp();

    StatusOr<std::vector<storage::cpp2::EdgeProp>> fillEdgeProp(
        const std::unordered_set<EdgeType> &edges);

    StatusOr<std::vector<storage::cpp2::EdgeProp>> buildAllEdgeProp();

private:
    std::unordered_set<EdgeType>                edgeTypes_;
};
}  // namespace graph
}  // namespace nebula
#endif
