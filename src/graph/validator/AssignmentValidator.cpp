/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/AssignmentValidator.h"

#include "parser/TraverseSentences.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

Status AssignmentValidator::validateImpl() {
    auto* assignSentence = static_cast<AssignmentSentence*>(sentence_);
    NG_RETURN_IF_ERROR(validator_->validate());

    auto outputs = validator_->outputCols();
    var_ = *assignSentence->var();
    vctx_->registerVariable(var_, std::move(outputs));
    return Status::OK();
}

Status AssignmentValidator::toPlan() {
    root_ = validator_->root();
    auto *var = qctx_->symTable()->newVariable(var_);
    for (const auto &outputCol : validator_->outputCols()) {
        var->colNames.emplace_back(outputCol.name);
    }
    root_->setOutputVar(var_);
    tail_ = validator_->tail();
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
