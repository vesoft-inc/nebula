/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/RemoveEdgeExecutor.h"

namespace nebula {
namespace graph {

RemoveEdgeExecutor::RemoveEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<RemoveEdgeSentence*>(sentence);
}

Status RemoveEdgeExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void RemoveEdgeExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();
    // dependent on MetaClient's removeEdge
    UNUSED(mc); UNUSED(name); UNUSED(spaceId);
}

}   // namespace graph
}   // namespace nebula
