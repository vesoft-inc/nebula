/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropSpaceExecutor.h"

namespace nebula {
namespace graph {

DropSpaceExecutor::DropSpaceExecutor(Sentence *sentence,
                                     ExecutionContext *ectx)
    : Executor(ectx, "drop_space") {
    sentence_ = static_cast<DropSpaceSentence*>(sentence);
}


Status DropSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    return Status::OK();
}


void DropSpaceExecutor::execute() {
    auto future = ectx()->getMetaClient()->dropSpace(*spaceName_, sentence_->isIfExists());
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(std::move(resp).status());
            return;
        }
        auto  ret = std::move(resp).value();
        if (!ret) {
            doError(Status::Error("Drop space `%s' failed.", spaceName_->c_str()));
            return;
        }

        if (*spaceName_ == ectx()->rctx()->session()->spaceName()) {
            ectx()->rctx()->session()->setSpace("", -1);
        }
        ectx()->addWarningMsg("If auto_remove_invalid_space is true, data will be deleted "
                              "completely after restarting the storage services, else you "
                              "need to remove data by manual");

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Drop space `%s' exception: %s",
                spaceName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}
}   // namespace graph
}   // namespace nebula
