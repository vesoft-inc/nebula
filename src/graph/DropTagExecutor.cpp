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
            doError(Status::Error("Drop tag `%s' failed.", sentence_->name()->c_str()));
            return;
        }
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Drop tag `%s' exception: %s",
                sentence_->name()->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

