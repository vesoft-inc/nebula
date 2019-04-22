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
    hosts_ = sentence_->hosts();
    if (hosts_.size() == 0) {
        return Status::Error("Host address illegal");
    }
    return Status::OK();
}


void AddHostsExecutor::execute() {
    auto future = ectx()->getMetaClient()->addHosts(hosts_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("Add hosts failed"));
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
