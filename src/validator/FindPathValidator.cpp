/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FindPathValidator.h"

#include "common/expression/VariableExpression.h"
#include "planner/plan/Algo.h"
#include "planner/plan/Logic.h"

namespace nebula {
namespace graph {
Status FindPathValidator::validateImpl() {
    auto fpSentence = static_cast<FindPathSentence*>(sentence_);
    pathCtx_ = getContext<PathContext>();
    pathCtx_->isShortest = fpSentence->isShortest();
    pathCtx_->noLoop = fpSentence->noLoop();
    pathCtx_->withProp = fpSentence->withProperites();
    pathCtx_->inputVarName = inputVarName_;
    NG_RETURN_IF_ERROR(validateStarts(fpSentence->from(), pathCtx_->from));
    NG_RETURN_IF_ERROR(validateStarts(fpSentence->to(), pathCtx_->to));
    NG_RETURN_IF_ERROR(validateOver(fpSentence->over(), pathCtx_->over));
    NG_RETURN_IF_ERROR(validateStep(fpSentence->step(), pathCtx_->steps));

    outputs_.emplace_back("path", Value::Type::PATH);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
