/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_SENDBLOCKSIGNPROCESSOR_H_
#define STORAGE_ADMIN_SENDBLOCKSIGNPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/Part.h"
#include "storage/StorageFlags.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {
class SendBlockSignProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static SendBlockSignProcessor* instance(StorageEnv* env) {
        return new SendBlockSignProcessor(env);
    }

    void process(const cpp2::BlockingSignRequest& req);

private:
    explicit SendBlockSignProcessor(StorageEnv* env)
            : BaseProcessor<cpp2::AdminExecResp>(env) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_SENDBLOCKSIGNPROCESSOR_H_
