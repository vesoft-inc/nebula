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
    lValidator_ = makeValidator(setSentence->left(), qctx_);
    auto status = lValidator_->validate();
    if (!status.ok()) {
        return status;
    }
    rValidator_ = makeValidator(setSentence->right(), qctx_);
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
    auto* plan = qctx_->plan();
    switch (op_) {
        case SetSentence::Operator::UNION: {
            auto unionOp = Union::make(
                    plan, lValidator_->root(), rValidator_->root());
            if (distinct_) {
                auto dedup = Dedup::make(
                        plan,
                        lValidator_->root(),
                        nullptr/* TODO: build condition*/);
                root_ = dedup;
            } else {
                root_ = unionOp;
            }
            break;
        }
        case SetSentence::Operator::INTERSECT: {
            root_ = Intersect::make(
                    plan,
                    lValidator_->root(),
                    rValidator_->root());
            break;
        }
        case SetSentence::Operator::MINUS: {
            root_ = Minus::make(
                    plan,
                    lValidator_->root(),
                    rValidator_->root());
            break;
        }
        default:
            return Status::Error("Unkown operator: %ld", static_cast<int64_t>(op_));
    }

    tail_ = MultiOutputsNode::make(plan, nullptr);
    Validator::appendPlan(lValidator_->tail(), tail_);
    Validator::appendPlan(rValidator_->tail(), tail_);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
