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
#include "meta/SchemaManager.h"

namespace nebula {
namespace graph {

class ExecutionPlan;

class ASTValidator final {
public:
    ASTValidator(SequentialSentences* sentences,
                 ClientSession* session,
                 meta::SchemaManager* schemaMng)
        : sentences_(sentences), session_(session), schemaMng_(schemaMng) {}

    Status validate(ExecutionPlan* plan);

    const ValidateContext* context() const {
        return validateContext_.get();
    }

private:
    SequentialSentences*                sentences_{nullptr};
    ClientSession*                      session_{nullptr};
    meta::SchemaManager*                schemaMng_{nullptr};
    std::unique_ptr<ValidateContext>    validateContext_;
};
}  // namespace graph
}  // namespace nebula
#endif
