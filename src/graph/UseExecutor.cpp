/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/UseExecutor.h"

namespace nebula {
namespace graph {

UseExecutor::UseExecutor(Sentence *sentence, ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<UseSentence*>(sentence);
}


Status UseExecutor::prepare() {
    return Status::OK();
}


void UseExecutor::execute() {
    auto future = ectx()->getMetaClient()->getSpace(*sentence_->space());
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("Space not found for `%s'", sentence_->space()->c_str()));
            return;
        }

        auto spaceId = resp.value().get_space_id();
        ectx()->rctx()->session()->setSpace(*sentence_->space(), spaceId);
        FLOG_INFO("Graph space switched to `%s', space id: %d",
                   sentence_->space()->c_str(), spaceId);

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
