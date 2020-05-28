/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/PipeValidator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {
Status PipeValidator::validateImpl() {
    auto pipeSentence = static_cast<PipedSentence*>(sentence_);
    auto left = pipeSentence->left();
    lValidator_ = makeValidator(left, validateContext_);
    auto status = lValidator_->validate();
    if (!status.ok()) {
        return status;
    }

    auto right = pipeSentence->right();
    rValidator_ = makeValidator(right, validateContext_);
    rValidator_->setInputs(lValidator_->outputs());
    status = rValidator_->validate();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status PipeValidator::toPlan() {
    root_ = rValidator_->root();
    auto status = Validator::appendPlan(rValidator_->tail(), lValidator_->root());
    if (!status.ok()) {
        return status;
    }
    tail_ = lValidator_->tail();
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
