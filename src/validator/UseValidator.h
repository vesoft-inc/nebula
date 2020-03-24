/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_USEVALIDATOR_H_
#define VALIDATOR_USEVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {
class UseValidator final : public Validator {
public:
    UseValidator(Sentence* sentence, ValidateContext* context)
        : Validator(sentence, context) {}

private:
    /**
     * Will not check the space for use space sentence.
     */
    bool spaceChosen() override {
        return true;
    }

    Status validateImpl() override;

    Status toPlan() override;
};
}  // namespace graph
}  // namespace nebula
#endif
