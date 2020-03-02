/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASK_H_
#define STORAGE_ADMIN_ADMINTASK_H_

#include "kvstore/Common.h"
#include "common/base/ThriftTypes.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace storage {

class AdminTask {
public:
    AdminTask() = default;
    virtual nebula::kvstore::ResultCode run() = 0;
    virtual nebula::kvstore::ResultCode stop() = 0;
    virtual ~AdminTask() {}
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask> createAdminTask(
            const nebula::storage::cpp2::AddAdminTaskRequest& req,
            nebula::kvstore::NebulaStore* store);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASK_H_
