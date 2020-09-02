/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FindPathValidator.h"
#include "planner/Logic.h"

namespace nebula {
namespace graph {
Status FindPathValidator::validateImpl() {
    auto fpSentence = static_cast<FindPathSentence*>(sentence_);
    isShortest_ = fpSentence->isShortest();

    NG_RETURN_IF_ERROR(validateStarts(fpSentence->from(), from_));
    NG_RETURN_IF_ERROR(validateStarts(fpSentence->to(), to_));
    NG_RETURN_IF_ERROR(validateOver(fpSentence->over(), over_));
    NG_RETURN_IF_ERROR(validateStep(fpSentence->step(), steps_));
    return Status::OK();
}

Status FindPathValidator::toPlan() {
    // TODO: Implement the path plan.
    auto* passThrough = PassThroughNode::make(qctx_, nullptr);
    tail_ = passThrough;
    root_ = tail_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
