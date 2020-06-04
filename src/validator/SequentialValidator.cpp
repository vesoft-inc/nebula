/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "validator/SequentialValidator.h"

namespace nebula {
namespace graph {
Status SequentialValidator::validateImpl() {
    Status status;
    if (sentence_->kind() != Sentence::Kind::kSequential) {
        return Status::Error(
                "Sequential validator validates a SequentialSentences, but %ld is given.",
                static_cast<int64_t>(sentence_->kind()));
    }
    auto seqSentence = static_cast<SequentialSentences*>(sentence_);
    auto sentences = seqSentence->sentences();
    for (auto* sentence : sentences) {
        auto validator = makeValidator(sentence, qctx_);
        status = validator->validate();
        if (!status.ok()) {
            return status;
        }
        validators_.emplace_back(std::move(validator));
    }

    return Status::OK();
}

Status SequentialValidator::toPlan() {
    root_ = validators_.back()->root();
    for (decltype(validators_.size()) i = 0; i < (validators_.size() - 1); ++i) {
        auto status = Validator::appendPlan(validators_[i + 1]->tail(), validators_[i]->root());
        if (!status.ok()) {
            return status;
        }
    }
    tail_ = validators_[0]->tail();
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
