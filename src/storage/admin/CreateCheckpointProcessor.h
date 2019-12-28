/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_CREATECHECKPOINTPROCESSOR_H_
#define STORAGE_ADMIN_CREATECHECKPOINTPROCESSOR_H_
#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {
class CreateCheckpointProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static CreateCheckpointProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateCheckpointProcessor(kvstore);
    }

    void process(const cpp2::CreateCPRequest& req);

private:
    explicit CreateCheckpointProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_CREATECHECKPOINTPROCESSOR_H_
