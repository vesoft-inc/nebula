/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ASTValidator.h"

namespace nebula {
namespace graph {
StatusOr<std::unique_ptr<ExecutionPlan>> ASTValidator::validate() {
    // CHECK(!!session_);
    // CHECK(!!schemaMng_);
    validateContext_ = std::make_unique<ValidateContext>();
    auto plan = std::make_unique<ExecutionPlan>();
    validateContext_->setPlan(plan.get());
    validateContext_->setSession(session_);
    validateContext_->setSchemaMng(schemaMng_);

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
    return plan;
}
}  // namespace graph
}  // namespace nebula
