/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/FetchVerticesExecutor.h"

namespace nebula {
namespace graph {
FetchVerticesExecutor::FetchVerticesExecutor(Sentence *sentence, ExecutionContext *ectx)
        :TraverseExecutor(ectx) {
    UNUSED(sentence);
}

Status FetchVerticesExecutor::prepare() {
    return Status::OK();
}

void FetchVerticesExecutor::execute() {
}

void FetchVerticesExecutor::feedResult(std::unique_ptr<InterimResult> result) {
    UNUSED(result);
}

void FetchVerticesExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    UNUSED(resp);
}
}  // namespace graph
}  // namespace nebula
