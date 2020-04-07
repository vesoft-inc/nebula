/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_GOVALIDATOR_H_
#define VALIDATOR_GOVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"

namespace nebula {
namespace graph {
class GoValidator final : public Validator {
public:
    GoValidator(Sentence* sentence, ValidateContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override;

    Status toPlan() override;

    Status validateStep();

    Status validateFrom();

    Status validateOver();

    Status validateWhere();

    Status validateYield();
};
}  // namespace graph
}  // namespace nebula
#endif
