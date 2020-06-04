/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include "common/base/Base.h"
#include "planner/ExecutionPlan.h"
#include "parser/Sentence.h"
#include "context/ValidateContext.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {
class Validator {
public:
    Validator(Sentence* sentence, QueryContext* qctx)
        : sentence_(sentence), qctx_(qctx), vctx_(qctx->vctx()) {}

    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    QueryContext* context);

    static Status appendPlan(PlanNode* plan, PlanNode* appended);

    Status validate();

    void setInputs(ColsDef&& inputs) {
        inputs_ = std::move(inputs);
    }

    auto root() const {
        return root_;
    }

    auto tail() const {
        return tail_;
    }

    auto outputs() const {
        return outputs_;
    }

    auto inputs() const {
        return inputs_;
    }

    void setNoSpaceRequired() {
        noSpaceRequired_ = true;
    }

protected:
    /**
     * Check if a space is chosen for this sentence.
     */
    virtual bool spaceChosen();

    /**
     * Validate the sentence.
     */
    virtual Status validateImpl() = 0;

    /**
     * Convert an ast to plan.
     */
    virtual Status toPlan() = 0;

protected:
    Sentence*                       sentence_{nullptr};
    QueryContext*                   qctx_{nullptr};
    ValidateContext*                vctx_{nullptr};
    PlanNode*                       root_{nullptr};
    PlanNode*                       tail_{nullptr};
    ColsDef                         outputs_;
    ColsDef                         inputs_;
    bool                            noSpaceRequired_{false};
};
}  // namespace graph
}  // namespace nebula
#endif
