/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_LIMITVALIDATOR_H_
#define VALIDATOR_LIMITVALIDATOR_H_

#include "validator/Validator.h"

namespace nebula {
namespace graph {
class LimitValidator final : public Validator {
public:
    LimitValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    int64_t                    offset_{-1};
    int64_t                    count_{-1};
};
}  // namespace graph
}  // namespace nebula
#endif  // VALIDATOR_LIMITVALIDATOR_H_
