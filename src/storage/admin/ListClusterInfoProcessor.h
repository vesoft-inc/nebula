/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_LISTCLUSTERINFO_H_
#define STORAGE_ADMIN_LISTCLUSTERINFO_H_

#include "common/base/Base.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class ListClusterInfoProcessor : public BaseProcessor<cpp2::ListClusterInfoResp> {
public:
    static ListClusterInfoProcessor* instance(StorageEnv* env) {
        return new ListClusterInfoProcessor(env);
    }

    void process(const cpp2::ListClusterInfoReq& req);

private:
    explicit ListClusterInfoProcessor(StorageEnv* env)
        : BaseProcessor<cpp2::ListClusterInfoResp>(env) {}
};

}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_ADMIN_LISTCLUSTERINFO_H_
