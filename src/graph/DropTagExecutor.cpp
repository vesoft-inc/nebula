/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropTagExecutor.h"

namespace nebula {
namespace graph {

DropTagExecutor::DropTagExecutor(Sentence *sentence,
                                 ExecutionContext *ectx)
    : Executor(ectx, "drop_tag") {
    sentence_ = static_cast<DropTagSentence*>(sentence);
}

Status DropTagExecutor::prepare() {
    return Status::OK();
}

void DropTagExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = mc->dropTagSchema(spaceId, *name, sentence_->isIfExists());

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(std::move(resp).status());
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

