/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GETSUBGRAPHVALIDATOR_H_
#define VALIDATOR_GETSUBGRAPHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/Clauses.h"

namespace nebula {
namespace graph {
class GetSubgraphValidator final : public Validator {
public:
    GetSubgraphValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateStep(StepClause* step);

    Status validateFrom(FromClause* from);

    Status validateInBound(InBoundClause* in);

    Status validateOutBound(OutBoundClause* out);

    Status validateBothInOutBound(BothInOutClause* out);

private:
    uint32_t                                    steps_{1};
    std::vector<Value>                          starts_;
    Expression*                                 srcRef_{nullptr};
    std::unordered_set<EdgeType>                edgeTypes_;
};
}  // namespace graph
}  // namespace nebula
#endif
