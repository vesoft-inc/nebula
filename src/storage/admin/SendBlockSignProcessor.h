/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_SENDBLOCKSIGNPROCESSOR_H_
#define STORAGE_ADMIN_SENDBLOCKSIGNPROCESSOR_H_
#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/Part.h"
#include "storage/StorageFlags.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {
class SendBlockSignProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static SendBlockSignProcessor* instance(kvstore::KVStore* kvstore) {
        return new SendBlockSignProcessor(kvstore);
    }

    void process(const cpp2::BlockingSignRequest& req);

private:
    explicit SendBlockSignProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_SENDBLOCKSIGNPROCESSOR_H_
