/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_BALANCEVALIDATOR_H_
#define VALIDATOR_BALANCEVALIDATOR_H_

#include "validator/Validator.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {

class BalanceValidator final : public Validator {
public:
    BalanceValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        setNoSpaceRequired();
    }

private:
    Status validateImpl() override {
        return Status::OK();
    }

    Status toPlan() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // VALIDATOR_BALANCEVALIDATOR_H_
