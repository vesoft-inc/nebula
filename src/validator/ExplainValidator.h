/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_EXPLAINVALIDATOR_H
#define VALIDATOR_EXPLAINVALIDATOR_H

#include <memory>

#include "validator/Validator.h"

namespace nebula {
namespace graph {

class SequentialValidator;

class ExplainValidator final : public Validator {
public:
    ExplainValidator(Sentence* sentence, QueryContext* context);

private:
    Status validateImpl() override;

    Status toPlan() override;

    std::unique_ptr<SequentialValidator> validator_;
};

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_EXPLAINVALIDATOR_H
