/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SetValidator.h"

#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status SetValidator::validateImpl() {
    auto setSentence = static_cast<SetSentence *>(sentence_);
    lValidator_ = makeValidator(setSentence->left(), qctx_);
    NG_RETURN_IF_ERROR(lValidator_->validate());
    rValidator_ = makeValidator(setSentence->right(), qctx_);
    NG_RETURN_IF_ERROR(rValidator_->validate());

    auto lCols = lValidator_->outputCols();
    auto rCols = rValidator_->outputCols();

    if (lCols.size() != rCols.size()) {
        return Status::SemanticError("number of columns to UNION/INTERSECT/MINUS must be same");
    }

    for (size_t i = 0, e = lCols.size(); i < e; i++) {
        if (lCols[i].first != rCols[i].first) {
            return Status::SemanticError(
                "different column names to UNION/INTERSECT/MINUS are not supported");
        }
    }

    outputs_ = std::move(lCols);
    return Status::OK();
}

Status SetValidator::toPlan() {
    auto plan = qctx_->plan();
    auto setSentence = static_cast<const SetSentence *>(sentence_);
    auto lRoot = DCHECK_NOTNULL(lValidator_->root());
    auto rRoot = DCHECK_NOTNULL(rValidator_->root());
    auto colNames = lRoot->colNames();
    BiInputNode *bNode = nullptr;
    switch (setSentence->op()) {
        case SetSentence::Operator::UNION: {
            bNode = Union::make(plan, lRoot, rRoot);
            bNode->setColNames(std::move(colNames));
            if (setSentence->distinct()) {
                auto dedup = Dedup::make(plan, bNode);
                dedup->setInputVar(bNode->varName());
                dedup->setColNames(bNode->colNames());
                root_ = dedup;
            } else {
                root_ = bNode;
            }
            break;
        }
        case SetSentence::Operator::INTERSECT: {
            bNode = Intersect::make(plan, lRoot, rRoot);
            bNode->setColNames(std::move(colNames));
            root_ = bNode;
            break;
        }
        case SetSentence::Operator::MINUS: {
            bNode = Minus::make(plan, lRoot, rRoot);
            bNode->setColNames(std::move(colNames));
            root_ = bNode;
            break;
        }
        default:
            return Status::Error("Unknown operator: %ld", static_cast<int64_t>(setSentence->op()));
    }

    bNode->setLeftVar(lRoot->varName());
    bNode->setRightVar(rRoot->varName());

    tail_ = MultiOutputsNode::make(plan, nullptr);
    NG_RETURN_IF_ERROR(lValidator_->appendPlan(tail_));
    NG_RETURN_IF_ERROR(rValidator_->appendPlan(tail_));
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
