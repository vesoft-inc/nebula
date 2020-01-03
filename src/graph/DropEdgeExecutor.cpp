/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropEdgeExecutor.h"

namespace nebula {
namespace graph {

DropEdgeExecutor::DropEdgeExecutor(Sentence *sentence,
                                   ExecutionContext *ectx)
    : Executor(ectx, "drop_edge") {
    sentence_ = static_cast<DropEdgeSentence*>(sentence);
}

Status DropEdgeExecutor::prepare() {
    return Status::OK();
}

void DropEdgeExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = mc->dropEdgeSchema(spaceId, *name, sentence_->isIfExists());

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

