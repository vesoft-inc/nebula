/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/RenameSpaceExecutor.h"

namespace nebula {
namespace graph {

RenameSpaceExecutor::RenameSpaceExecutor(Sentence *sentence,
                                         ExecutionContext *ectx)
    : Executor(ectx) {
    sentence_ = static_cast<RenameSpaceSentence*>(sentence);
}


Status RenameSpaceExecutor::prepare() {
    fromSpaceName_ = sentence_->fromSpaceName();
    toSpaceName_ = sentence_->toSpaceName();
    if (fromSpaceName_ == nullptr || toSpaceName_ == nullptr) {
        return Status::Error("Input space name is empty");
    }
    return Status::OK();
}


void RenameSpaceExecutor::execute() {
    auto future = ectx()->getMetaClient()->renameSpace(*fromSpaceName_, *toSpaceName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            onError_(Status::Error("Rename space `%s' failed: %s.",
                                    fromSpaceName_->c_str(), resp.status().toString().c_str()));
            return;
        }

        // If current space is the rename space, need to clear the old space name in session
        if (*fromSpaceName_ == ectx()->rctx()->session()->spaceName()) {
            ectx()->rctx()->session()->setSpace(*toSpaceName_, ectx()->rctx()->session()->space());
        }
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Rename space `%s' exception: %s.",
                                        fromSpaceName_->c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        onError_(Status::Error(std::move(msg)));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

