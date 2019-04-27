/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/RemoveTagExecutor.h"

namespace nebula {
namespace graph {

RemoveTagExecutor::RemoveTagExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<RemoveTagSentence*>(sentence);
}

Status RemoveTagExecutor::prepare() {
    return Status::OK();
}

void RemoveTagExecutor::execute() {
    auto *sm = ectx()->schemaManager();
    auto *name = sentence_->name();
    auto space = ectx()->rctx()->session()->space();
    auto tagId = ectx()->schemaManager()->toTagID(*name);
    sm->removeTagSchema(space, tagId);
    DCHECK(onFinish_);
    onFinish_();
}

}   // namespace graph
}   // namespace nebula
