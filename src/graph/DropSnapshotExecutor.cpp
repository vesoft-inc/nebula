/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropSnapshotExecutor.h"

namespace nebula {
namespace graph {

DropSnapshotExecutor::DropSnapshotExecutor(Sentence *sentence,
        ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropSnapshotSentence*>(sentence);
}

Status DropSnapshotExecutor::prepare() {
    return Status::OK();
}

void DropSnapshotExecutor::execute() {
    auto *name = sentence_->getName();
    auto future = ectx()->getMetaClient()->dropSnapshot(*name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::forward<decltype(resp)>(resp).status());
            return;
        }
        auto ret = std::forward<decltype(resp)>(resp).value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("Balance leader failed"));
            return;
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
