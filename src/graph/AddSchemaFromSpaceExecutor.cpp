/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/AddSchemaFromSpaceExecutor.h"

namespace nebula {
namespace graph {

AddSchemaFromSpaceExecutor::AddSchemaFromSpaceExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
        : Executor(ectx, "add_schema_from_space") {
    sentence_ = static_cast<AddSchemaFromSpaceSentence*>(sentence);
}


Status AddSchemaFromSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    if (spaceName_ == nullptr) {
        return Status::Error("Empty space name");
    }
    return Status::OK();
}

void AddSchemaFromSpaceExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }
    auto currentSpace = ectx()->rctx()->session()->spaceName();
    if (currentSpace == *spaceName_) {
        LOG(ERROR) << "Current space `" << currentSpace << "' as same as from space `"
                   << spaceName_->c_str() << "'";
        doError(Status::Error("From space `%s' as same as current space `%s'",
                spaceName_->c_str(), currentSpace.c_str()));
        return;
    }

    auto future = ectx()->getMetaClient()->addSchemaFromSpace(currentSpace, *spaceName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            LOG(ERROR) << "Add schema from space `" << spaceName_->c_str()
                       << "' failed: " << resp.status().toString().c_str();
            doError(Status::Error("Add schema from space `%s' failed: %s",
                                  spaceName_->c_str(), resp.status().toString().c_str()));
            return;
        }

        LOG(INFO) << "Add schema from space `" << spaceName_->c_str() << "' succeed";
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Add schema from space `%s' exception: %s.",
                                       spaceName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}
}   // namespace graph
}   // namespace nebula

