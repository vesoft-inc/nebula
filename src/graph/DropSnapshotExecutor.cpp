/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropSnapshotExecutor.h"

namespace nebula {
namespace graph {

DropSnapshotExecutor::DropSnapshotExecutor(Sentence *sentence,
        ExecutionContext *ectx) : Executor(ectx, "drop_snapshot") {
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
            doError(std::forward<decltype(resp)>(resp).status());
            return;
        }
        auto ret = std::forward<decltype(resp)>(resp).value();
        if (!ret) {
            doError(Status::Error("Balance leader failed"));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Drop snapshot exception: " << e.what();
        doError(Status::Error("Drop snapshot exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
