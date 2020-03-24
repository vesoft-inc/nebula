/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SetValidator.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Status SetValidator::validateImpl() {
    auto setSentence = static_cast<SetSentence*>(sentence_);
    lValidator_ = Validator::makeValidator(setSentence->left(), validateContext_);
    auto status = lValidator_->validate();
    if (!status.ok()) {
        return status;
    }
    rValidator_ = Validator::makeValidator(setSentence->right(), validateContext_);
    status = rValidator_->validate();
    if (!status.ok()) {
        return status;
    }

    auto lCols = lValidator_->outputs();
    auto rCols = rValidator_->outputs();
    if (lCols.size() != rCols.size()) {
        return Status::Error("The used statements have a diffrent number of columns.");
    }

    outputs_ = std::move(lCols);
    return Status::OK();
}

Status SetValidator::toPlan() {
    switch (op_) {
        case SetSentence::Operator::UNION:
            start_ = std::make_shared<Union>(distinct_);
            break;
        case SetSentence::Operator::INTERSECT:
            start_ = std::make_shared<Intersect>();
            break;
        case SetSentence::Operator::MINUS:
            start_ = std::make_shared<Minus>();
            break;
        default:
            return Status::Error("Unkown operator: %ld", static_cast<int64_t>(op_));
    }

    start_->addChild(lValidator_->start());
    start_->addChild(lValidator_->start());
    end_ = std::make_shared<EndNode>();
    lValidator_->end()->addChild(end_);
    rValidator_->end()->addChild(end_);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
