/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/FetchEdgesExecutor.h"

namespace nebula {
namespace graph {
FetchEdgesExecutor::FetchEdgesExecutor(Sentence *sentence, ExecutionContext *ectx)
        :TraverseExecutor(ectx) {
    UNUSED(sentence);
}

Status FetchEdgesExecutor::prepare() {
    return Status::OK();
}

void FetchEdgesExecutor::execute() {
}

void FetchEdgesExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    UNUSED(result);
}

void FetchEdgesExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    UNUSED(resp);
}
}  // namespace graph
}  // namespace nebula
