/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/RefreshMetaCacheExecutor.h"

namespace nebula {
namespace graph {

RefreshMetaCacheExecutor::RefreshMetaCacheExecutor(Sentence *sentence,
                                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<RefreshMetaCacheSentence*>(sentence);
}


Status RefreshMetaCacheExecutor::prepare() {
    return Status::OK();
}


// TODO(YT)  refresh all meta client cache
void RefreshMetaCacheExecutor::execute() {
    auto future = ectx()->getMetaClient()->refreshCache();
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
            onError_(Status::Error("Refresh meta cache failed"));
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
