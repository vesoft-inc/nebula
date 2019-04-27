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
    return Status::OK();
}

void RemoveEdgeExecutor::execute() {
    auto *sm = ectx()->schemaManager();
    auto *name = sentence_->name();
    auto space = ectx()->rctx()->session()->space();
    auto edgeType = ectx()->schemaManager()->toEdgeType(*name);
    sm->removeEdgeSchema(space, edgeType);
    DCHECK(onFinish_);
    onFinish_();
}

}   // namespace graph
}   // namespace nebula
