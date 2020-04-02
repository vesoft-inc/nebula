/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "PipeValidator.h"
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
    start_ = rValidator_->start();
    auto status = Validator::appendPlan(rValidator_->end(), lValidator_->start());
    if (!status.ok()) {
        return status;
    }
    end_ = lValidator_->end();
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
