/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_KV_REMOVEPROCESSOR_H_
#define STORAGE_KV_REMOVEPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class RemoveProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static RemoveProcessor* instance(StorageEnv* env, stats::Stats* stats) {
        return new RemoveProcessor(env, stats);
    }

    void process(const cpp2::KVRemoveRequest& req);

private:
    explicit RemoveProcessor(StorageEnv* env, stats::Stats* stats)
            : BaseProcessor<cpp2::ExecResponse>(env, stats) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_REMOVEPROCESSOR_H_
