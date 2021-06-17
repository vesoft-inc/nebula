/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SetValidator.h"

#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

Status SetValidator::validateImpl() {
    NG_RETURN_IF_ERROR(lValidator_->validate());
    NG_RETURN_IF_ERROR(rValidator_->validate());

    auto lCols = lValidator_->outputCols();
    auto rCols = rValidator_->outputCols();

    if (lCols.size() != rCols.size()) {
        return Status::SemanticError("number of columns to UNION/INTERSECT/MINUS must be same");
    }

    for (size_t i = 0, e = lCols.size(); i < e; i++) {
        if (lCols[i].name != rCols[i].name) {
            return Status::SemanticError(
                "different column names to UNION/INTERSECT/MINUS are not supported");
        }
    }

    outputs_ = std::move(lCols);
    return Status::OK();
}

Status SetValidator::toPlan() {
    auto setSentence = static_cast<const SetSentence *>(sentence_);
    auto lRoot = DCHECK_NOTNULL(lValidator_->root());
    auto rRoot = DCHECK_NOTNULL(rValidator_->root());
    auto colNames = lRoot->colNames();
    BinaryInputNode *bNode = nullptr;
    switch (setSentence->op()) {
        case SetSentence::Operator::UNION: {
            bNode = Union::make(qctx_, lRoot, rRoot);
            bNode->setColNames(std::move(colNames));
            if (setSentence->distinct()) {
                root_ = Dedup::make(qctx_, bNode);
            } else {
                root_ = bNode;
            }
            break;
        }
        case SetSentence::Operator::INTERSECT: {
            bNode = Intersect::make(qctx_, lRoot, rRoot);
            bNode->setColNames(colNames);
            root_ = bNode;
            break;
        }
        case SetSentence::Operator::MINUS: {
            bNode = Minus::make(qctx_, lRoot, rRoot);
            bNode->setColNames(colNames);
            root_ = bNode;
            break;
        }
        default:
            return Status::SemanticError("Unknown operator: %ld",
                                         static_cast<int64_t>(setSentence->op()));
    }

    bNode->setLeftVar(lRoot->outputVar());
    bNode->setRightVar(rRoot->outputVar());

    tail_ = PassThroughNode::make(qctx_, nullptr);
    NG_RETURN_IF_ERROR(lValidator_->appendPlan(tail_));
    NG_RETURN_IF_ERROR(rValidator_->appendPlan(tail_));
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
