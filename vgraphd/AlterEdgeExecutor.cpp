/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/AlterEdgeExecutor.h"

namespace vesoft {
namespace vgraph {

AlterEdgeExecutor::AlterEdgeExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AlterEdgeSentence*>(sentence);
}


Status AlterEdgeExecutor::prepare() {
    return Status::OK();
}


void AlterEdgeExecutor::execute() {
    if (onFinish_) {
        onFinish_();
    }
}

}   // namespace vgraph
}   // namespace vesoft
