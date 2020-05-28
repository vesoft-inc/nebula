/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_DROPCHECKPOINTPROCESSOR_H_
#define STORAGE_ADMIN_DROPCHECKPOINTPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {
class DropCheckpointProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static DropCheckpointProcessor* instance(StorageEnv* env) {
        return new DropCheckpointProcessor(env);
    }

    void process(const cpp2::DropCPRequest& req);

private:
    explicit DropCheckpointProcessor(StorageEnv* env)
            : BaseProcessor<cpp2::AdminExecResp>(env) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_DROPCHECKPOINTPROCESSOR_H_

