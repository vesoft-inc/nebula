/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CreateSnapshotExecutor.h"

namespace nebula {
namespace graph {

CreateSnapshotExecutor::CreateSnapshotExecutor(Sentence *sentence,
                                               ExecutionContext *ectx)
    : Executor(ectx, "create_snapshot") {
    sentence_ = static_cast<CreateSnapshotSentence*>(sentence);
}

Status CreateSnapshotExecutor::prepare() {
    return Status::OK();
}

void CreateSnapshotExecutor::execute() {
    auto future = ectx()->getMetaClient()->createSnapshot();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(std::move(resp).status());
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            doError(Status::Error("Create snapshot failed"));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Create snapshot exception: " << e.what();
        doError(Status::Error("Create snapshot exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
