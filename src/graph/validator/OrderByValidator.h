/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ORDERBYVALIDATOR_H_
#define VALIDATOR_ORDERBYVALIDATOR_H_

#include "validator/Validator.h"

namespace nebula {
namespace graph {
class OrderByValidator final : public Validator {
public:
    OrderByValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    std::vector<std::pair<size_t, OrderFactor::OrderType>>     colOrderTypes_;
};
}  // namespace graph
}  // namespace nebula
#endif   // VALIDATOR_ORDERBYVALIDATOR_H_
