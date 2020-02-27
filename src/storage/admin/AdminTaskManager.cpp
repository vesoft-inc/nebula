/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/admin/AdminTask.h"
#include "storage/admin/AdminTaskManager.h"

namespace nebula {
namespace storage {

// JobManager can guarantee there won't be more than 1 task
nebula::kvstore::ResultCode
AdminTaskManager::addTask(const cpp2::AddAdminTaskRequest& req,
                          nebula::kvstore::NebulaStore* store) {
    auto adminTask = AdminTaskFactory::createAdminTask(req, store);
    // if support stop/cancel a task
    // there should be a queue here
    // else return directly
    return adminTask->run();
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelTask(const cpp2::AddAdminTaskRequest& req) {
    UNUSED(req);
    return nebula::kvstore::ResultCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula


