/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/FindExecutor.h"

namespace nebula {
namespace graph {

FindExecutor::FindExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = static_cast<FindSentence*>(sentence);
}


Status FindExecutor::prepare() {
    return Status::Error("Does not support");
}

}   // namespace graph
}   // namespace nebula
