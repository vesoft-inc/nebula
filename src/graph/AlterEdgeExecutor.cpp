/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/AlterEdgeExecutor.h"

namespace nebula {
namespace graph {

AlterEdgeExecutor::AlterEdgeExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AlterEdgeSentence*>(sentence);
}


Status AlterEdgeExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void AlterEdgeExecutor::execute() {
    if (onFinish_) {
        onFinish_();
    }
}

}   // namespace graph
}   // namespace nebula
