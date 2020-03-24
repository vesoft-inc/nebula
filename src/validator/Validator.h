/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATOR_H_
#define VALIDATOR_VALIDATOR_H_

#include "base/Base.h"
#include "planner/PlanNode.h"
#include "parser/Sentence.h"
#include "validator/ValidateContext.h"

namespace nebula {
namespace graph {
class Validator {
public:
    explicit Validator(Sentence* sentence, ValidateContext* context)
        : sentence_(sentence), validateContext_(context) {}

    virtual ~Validator() = default;

    static std::unique_ptr<Validator> makeValidator(Sentence* sentence,
                                                    ValidateContext* context);

    Status validate();

    void setInputs(ColsDef&& inputs) {
        inputs_ = std::move(inputs);
    }

    auto start() const {
        return start_;
    }

    auto end() const {
        return end_;
    }

    auto outputs() const {
        return outputs_;
    }

    auto inputs() const {
        return inputs_;
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
    Sentence*                       sentence_;
    ValidateContext*                validateContext_;
    std::shared_ptr<PlanNode>       start_;
    std::shared_ptr<PlanNode>       end_;
    ColsDef                         outputs_;
    ColsDef                         inputs_;
};
}  // namespace graph
}  // namespace nebula
#endif
