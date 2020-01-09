/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropEdgeIndexExecutor.h"

namespace nebula {
namespace graph {

DropEdgeIndexExecutor::DropEdgeIndexExecutor(Sentence *sentence,
                                             ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropEdgeIndexSentence*>(sentence);
}

Status DropEdgeIndexExecutor::prepare() {
    return Status::OK();
}

void DropEdgeIndexExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    auto *name = sentence_->indexName();
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = ectx()->getMetaClient()->dropEdgeIndex(spaceId, *name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("Index not found for tag '%s'",
                                   sentence_->indexName()->c_str()));
            return;
        }

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

