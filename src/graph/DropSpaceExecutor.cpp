/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/DropSpaceExecutor.h"

namespace nebula {
namespace graph {

DropSpaceExecutor::DropSpaceExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropSpaceSentence*>(sentence);
}


Status DropSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    return Status::OK();
}


void DropSpaceExecutor::execute() {
    auto future = ectx()->getMetaClient()->dropSpace(*spaceName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        auto  ret = resp.value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("Drop space failed"));
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
}

}   // namespace graph
}   // namespace nebula
