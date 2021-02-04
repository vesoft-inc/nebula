/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_GetValueProcessor_H_
#define STORAGE_TRANSACTION_GetValueProcessor_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kGetValueCounters;

class GetValueProcessor : public BaseProcessor<cpp2::GetValueResponse> {
public:
    static GetValueProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kGetValueCounters) {
        return new GetValueProcessor(env, counters);
    }

    void process(const cpp2::GetValueRequest& req);

protected:
    explicit GetValueProcessor(StorageEnv* env, const ProcessorCounters* counters):
        BaseProcessor<cpp2::GetValueResponse>(env, counters) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_GetValueProcessor_H_
