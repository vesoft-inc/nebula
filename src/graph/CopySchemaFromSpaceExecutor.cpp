/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CopySchemaFromSpaceExecutor.h"

namespace nebula {
namespace graph {

CopySchemaFromSpaceExecutor::CopySchemaFromSpaceExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
        : Executor(ectx) {
    sentence_ = static_cast<CopySchemaFromSpaceSentence*>(sentence);
}

Status CopySchemaFromSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    return Status::OK();
}

void CopySchemaFromSpaceExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        onError_(std::move(status));
        return;
    }
    auto currentSpace = ectx()->rctx()->session()->spaceName();
    if (currentSpace == *spaceName_) {
        LOG(ERROR) << "Current space `" << currentSpace << "' as same as from space `"
                   << spaceName_->c_str() << "'";
        onError_(Status::Error("From space `%s' as same as current space `%s'",
                spaceName_->c_str(), currentSpace.c_str()));
        return;
    }

    auto future = ectx()->getMetaClient()->copySchemaFromSpace(
            currentSpace, *spaceName_, sentence_->needIndex());
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            LOG(ERROR) << "Copy schema from space `" << spaceName_->c_str()
                       << "' failed: " << resp.status().toString().c_str();
            onError_(Status::Error("Copy schema from space `%s' failed: %s",
                                   spaceName_->c_str(), resp.status().toString().c_str()));
            return;
        }

        LOG(INFO) << "Copy schema from space `" << spaceName_->c_str() << "' succeed";
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Copy schema from space `%s' exception: %s.",
                                        spaceName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        onError_(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}
}   // namespace graph
}   // namespace nebula

