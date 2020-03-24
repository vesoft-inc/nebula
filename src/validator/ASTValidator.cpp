/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ASTValidator.h"

namespace nebula {
namespace graph {
Status ASTValidator::validate(std::shared_ptr<PlanNode> plan) {
    validateContext_ = std::make_unique<ValidateContext>();

    // check space chosen from session. if chosen, add it to context.
    if (session_->space() > -1) {
        validateContext_->switchToSpace(session_->spaceName(), session_->space());
    }

    auto validator = Validator::makeValidator(sentences_, validateContext_.get());
    auto status = validator->validate();
    if (!status.ok()) {
        return status;
    }

    plan = validator->start();
    if (!plan) {
        return Status::Error("Get null plan from sequantial validator.");
    }

    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
