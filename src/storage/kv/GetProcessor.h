/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_KV_GETPROCESSOR_H_
#define STORAGE_KV_GETPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class GetProcessor : public BaseProcessor<cpp2::KVGetResponse> {
public:
    static GetProcessor* instance(StorageEnv* env, stats::Stats* stats) {
        return new GetProcessor(env, stats);
    }

    void process(const cpp2::KVGetRequest& req);

protected:
    explicit GetProcessor(StorageEnv* env, stats::Stats* stats):
        BaseProcessor<cpp2::KVGetResponse>(env, stats) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_GETPROCESSOR_H_
