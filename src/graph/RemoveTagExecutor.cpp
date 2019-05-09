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
    return checkIfGraphSpaceChosen();
}

void RemoveTagExecutor::execute() {
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();
    UNUSED(mc); UNUSED(name); UNUSED(spaceId);

    // TODO dependent on MetaClient's removeTagSchema (pull/295)
    /**
    auto future = mc->removeTagSchema(spaceId, *name);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
    **/
}

}   // namespace graph
}   // namespace nebula
