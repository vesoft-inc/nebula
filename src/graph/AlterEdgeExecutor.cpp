/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
