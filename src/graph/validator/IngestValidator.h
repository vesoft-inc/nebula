/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_INGESTVALIDATOR_H_
#define VALIDATOR_INGESTVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/Validator.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {

class IngestValidator final : public Validator {
public:
    IngestValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {}

private:
    Status validateImpl() override {
        return Status::OK();
    }

    Status toPlan() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // VALIDATOR_INGESTVALIDATOR_H_
