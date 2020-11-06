/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_ADMINTASKPROCESSOR_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace storage {

class AdminTaskProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static AdminTaskProcessor* instance(StorageEnv* env) {
        return new AdminTaskProcessor(env);
    }

    void process(const cpp2::AddAdminTaskRequest& req);

private:
    explicit AdminTaskProcessor(StorageEnv* env)
            : BaseProcessor<cpp2::AdminExecResp>(env) {}

    void onProcessFinished(nebula::meta::cpp2::StatisItem& result);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
