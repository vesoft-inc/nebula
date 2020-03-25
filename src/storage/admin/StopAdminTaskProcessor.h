/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

#include "base/ThriftTypes.h"

namespace nebula {
namespace storage {
class StopAdminTaskProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static StopAdminTaskProcessor* instance(kvstore::KVStore* kvstore) {
        return new StopAdminTaskProcessor(kvstore);
    }

    void process(const cpp2::StopAdminTaskRequest& req);

private:
    explicit StopAdminTaskProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
