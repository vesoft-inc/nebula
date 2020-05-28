/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ASTValidator.h"
#include "planner/ExecutionPlan.h"
#include "common/charset/Charset.h"

namespace nebula {
namespace graph {

Status ASTValidator::validate(ExecutionPlan* plan) {
    // CHECK(!!session_);
    // CHECK(!!schemaMng_);
    validateContext_ = std::make_unique<ValidateContext>();
    validateContext_->setPlan(plan);
    validateContext_->setSession(session_);
    validateContext_->setSchemaMng(schemaMng_);
    validateContext_->setCharsetInfo(charsetInfo_);

    // Check if space chosen from session. if chosen, add it to context.
    if (session_->space() > -1) {
        validateContext_->switchToSpace(session_->spaceName(), session_->space());
    }

    auto validator = Validator::makeValidator(sentences_, validateContext_.get());
    auto status = validator->validate();
    if (!status.ok()) {
        return status;
    }

    auto root = validator->root();
    if (!root) {
        return Status::Error("Get null plan from sequantial validator.");
    }

    plan->setRoot(root);
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
