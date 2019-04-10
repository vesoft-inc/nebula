/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/AddHostsExecutor.h"

namespace nebula {
namespace graph {

AddHostsExecutor::AddHostsExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AddHostsSentence*>(sentence);
}


Status AddHostsExecutor::prepare() {
    host_ = sentence_->hosts();
    if (host_.size() == 0) {
        return Status::Error("Add hosts Sentence host address illegal");
    }
    return Status::OK();
}


void AddHostsExecutor::execute() {
    auto future = ectx()->getMetaClient()->addHosts(host_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        auto ret = resp.value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("add hosts failed"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
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
