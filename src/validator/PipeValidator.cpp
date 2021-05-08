/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/PipeValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

Status PipeValidator::validateImpl() {
    lValidator_->setInputCols(std::move(inputs_));
    lValidator_->setInputVarName(inputVarName_);
    NG_RETURN_IF_ERROR(lValidator_->validate());

    rValidator_->setInputCols(lValidator_->outputCols());
    rValidator_->setInputVarName(lValidator_->root()->outputVar());
    NG_RETURN_IF_ERROR(rValidator_->validate());

    outputs_ = rValidator_->outputCols();
    return Status::OK();
}

Status PipeValidator::toPlan() {
    root_ = rValidator_->root();
    tail_ = lValidator_->tail();
    NG_RETURN_IF_ERROR(rValidator_->appendPlan(lValidator_->root()));
    auto node = static_cast<SingleInputNode*>(rValidator_->tail());
    if (node->inputVar().empty()) {
        // If the input variable was not set, set it dynamically.
        node->setInputVar(lValidator_->root()->outputVar());
    }
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
