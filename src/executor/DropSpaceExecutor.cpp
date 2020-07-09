/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropSpaceExecutor.h"

namespace nebula {
namespace graph {

DropSpaceExecutor::DropSpaceExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropSpaceSentence*>(sentence);
}


Status DropSpaceExecutor::prepare() {
    spaceName_ = sentence_->name();
    return Status::OK();
}


void DropSpaceExecutor::execute() {
    auto future = ectx()->getMetaClient()->dropSpace(*spaceName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        auto  ret = std::move(resp).value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("Drop space failed"));
            return;
        }

        if (*spaceName_ == ectx()->rctx()->session()->spaceName()) {
            ectx()->rctx()->session()->setSpace("", -1);
        }
        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
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
