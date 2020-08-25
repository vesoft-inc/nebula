/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_TRAVERSALVALIDATOR_H_
#define VALIDATOR_TRAVERSALVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

// some utils for the validator to traverse the graph
class TraversalValidator : public Validator {
protected:
    TraversalValidator(Sentence* sentence, QueryContext* qctx) : Validator(sentence, qctx) {}

    Status validateStep(const StepClause* step);
    Status validateFrom(const FromClause* from);

    PlanNode* projectDstVidsFromGN(PlanNode* gn, const std::string& outputVar);
    std::string buildConstantInput();
    PlanNode* buildRuntimeInput();
    Expression* buildNStepLoopCondition(uint32_t steps) const;

    enum FromType {
        kInstantExpr,
        kVariable,
        kPipe,
    };

protected:
    StepClause::MToN*     mToN_{nullptr};
    uint32_t              steps_{1};
    std::string           srcVidColName_;
    FromType              fromType_{kInstantExpr};
    Expression*           srcRef_{nullptr};
    Expression*           src_{nullptr};
    std::vector<Value>    starts_;
    std::string           firstBeginningSrcVidColName_;
    std::string           userDefinedVarName_;
    ExpressionProps       exprProps_;
    PlanNode*             projectStartVid_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif
