/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

#include "base/ThriftTypes.h"

namespace nebula {
namespace storage {
class AdminTaskProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static AdminTaskProcessor* instance(kvstore::KVStore* kvstore) {
        return new AdminTaskProcessor(kvstore);
    }

    void process(const cpp2::AddAdminTaskRequest& req);

private:
    explicit AdminTaskProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
