/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/RefreshCacheExecutor.h"

namespace nebula {
namespace graph {

RefreshCacheExecutor::RefreshCacheExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<RefreshCacheSentence*>(sentence);
}


Status RefreshCacheExecutor::prepare() {
    return Status::OK();
}


void RefreshCacheExecutor::execute() {
    auto ret = ectx()->getMetaClient()->refreshCache();

    if (!ret.ok()) {
        DCHECK(onError_);
        onError_(Status::Error("Refresh cache failed"));
        return;
    }

    DCHECK(onFinish_);
    onFinish_();
}

}   // namespace graph
}   // namespace nebula
