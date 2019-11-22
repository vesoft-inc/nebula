/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropEdgeExecutor.h"

namespace nebula {
namespace graph {

DropEdgeExecutor::DropEdgeExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropEdgeSentence*>(sentence);
}

Status DropEdgeExecutor::prepare() {
    return Status::OK();
}

void DropEdgeExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }
    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->name();
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = mc->dropEdgeSchema(spaceId, *name);

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
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

