/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ASTVALIDATOR_H_
#define VALIDATOR_ASTVALIDATOR_H_

#include "base/Base.h"
#include "validator/Validator.h"
#include "parser/SequentialSentences.h"

namespace nebula {
namespace graph {
class ASTValidator final {
public:
    explicit ASTValidator(SequentialSentences* sentences)
        : sentences_(sentences) {}

    StatusOr<std::unique_ptr<ExecutionPlan>> validate();

    const ValidateContext* context() const {
        return validateContext_.get();
    }

private:
    SequentialSentences*                sentences_;
    ClientSession*                      session_;
    std::unique_ptr<ValidateContext>    validateContext_;
};
}  // namespace graph
}  // namespace nebula
#endif
