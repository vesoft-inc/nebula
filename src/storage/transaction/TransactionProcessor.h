/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONPROCESSOR_H_
#define STORAGE_TRANSACTION_TRANSACTIONPROCESSOR_H_

#include <folly/FBVector.h>
#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kForwardTranxCounters;

class InterTxnProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static InterTxnProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kForwardTranxCounters) {
        return new InterTxnProcessor(env, counters);
    }

    void process(const cpp2::InternalTxnRequest& req);

private:
    InterTxnProcessor(StorageEnv* env, const ProcessorCounters* counters)
        : BaseProcessor<cpp2::ExecResponse>(env, counters) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTIONPROCESSOR_H_
