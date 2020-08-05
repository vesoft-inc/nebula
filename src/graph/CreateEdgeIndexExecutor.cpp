/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CreateEdgeIndexExecutor.h"

namespace nebula {
namespace graph {

CreateEdgeIndexExecutor::CreateEdgeIndexExecutor(Sentence *sentence, ExecutionContext *ectx)
    : Executor(ectx) {
    sentence_ = static_cast<CreateEdgeIndexSentence *>(sentence);
}

Status CreateEdgeIndexExecutor::prepare() {
    return Status::OK();
}

void CreateEdgeIndexExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(std::move(status));
        return;
    }

    auto *mc = ectx()->getMetaClient();
    const auto *name = sentence_->indexName();
    auto *edgeName = sentence_->edgeName();
    auto columns = sentence_->names();
    auto spaceId = ectx()->rctx()->session()->space();
    if (UNLIKELY(columns.empty())) {
        // It's not allowed by parser in normal
        LOG(WARNING) << "Impossible empty index fields.";
        onError_(Status::MalformedRequest("Empty fields."));
        return;
    }
    std::unordered_set<std::string> uniFields;
    uniFields.reserve(columns.size());
    uniFields.insert(columns.begin(), columns.end());
    if (UNLIKELY(uniFields.size() != columns.size())) {
        onError_(Status::MalformedRequest("Duplicate fields."));
        return;
    }

    auto future = mc->createEdgeIndex(
        spaceId, *name, *edgeName, std::move(columns), sentence_->isIfNotExist());
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this](auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(resp.status());
            return;
        }

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this](auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        onError_(Status::Error("Internal error"));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
