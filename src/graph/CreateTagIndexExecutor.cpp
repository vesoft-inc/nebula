/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CreateTagIndexExecutor.h"

namespace nebula {
namespace graph {

CreateTagIndexExecutor::CreateTagIndexExecutor(Sentence *sentence,
                                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateTagIndexSentence*>(sentence);
}

Status CreateTagIndexExecutor::prepare() {
    return Status::OK();
}

void CreateTagIndexExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    auto *mc = ectx()->getMetaClient();
    auto *name = sentence_->indexName();
    auto *tagName = sentence_->tagName();
    auto columns = sentence_->names();
    auto spaceId = ectx()->rctx()->session()->space();

    auto future = mc->createTagIndex(spaceId,
                                     *name,
                                     *tagName,
                                     columns,
                                     sentence_->isIfNotExist());
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

