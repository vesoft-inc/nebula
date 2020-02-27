/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKMANAGER_H_
#define STORAGE_ADMIN_ADMINTASKMANAGER_H_

#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class AdminTaskManager {
public:
    static AdminTaskManager& instance() {
        static AdminTaskManager sAdminTaskManager;
        return sAdminTaskManager;
    }

    nebula::kvstore::ResultCode addTask(const cpp2::AddAdminTaskRequest& req,
                            nebula::kvstore::NebulaStore* store);
    nebula::kvstore::ResultCode cancelTask(const cpp2::AddAdminTaskRequest& req);

private:
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKMANAGER_H_
