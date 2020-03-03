/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKMANAGER_H_
#define STORAGE_ADMIN_ADMINTASKMANAGER_H_

#include "interface/gen-cpp2/storage_types.h"
#include <condition_variable>
#include <mutex>
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class AdminTaskManager {
    using ResultCode = nebula::kvstore::ResultCode;

    using ASyncTaskRet = folly::Promise<ResultCode>;
    using TaskAndResult = std::pair<std::shared_ptr<AdminTask>, ASyncTaskRet>;
    using JobIdAndTaskId = std::pair<int, int>;
    using FutureResultCode = folly::Future<ResultCode>;

public:
    AdminTaskManager() = default;
    static AdminTaskManager& instance() {
        static AdminTaskManager sAdminTaskManager;
        return sAdminTaskManager;
    }

    ResultCode runTaskDirectly(const cpp2::AddAdminTaskRequest& req,
                               nebula::kvstore::NebulaStore* store);

    FutureResultCode addAsyncTask(const cpp2::AddAdminTaskRequest& req,
                                  nebula::kvstore::NebulaStore* store);

    ResultCode cancelTask(const cpp2::AddAdminTaskRequest& req);

    bool init();

    void shutdown();

private:
    void pickTaskThread();

private:
    std::unique_ptr<thread::GenericWorker> bgThread_;
    std::unique_ptr<nebula::thread::GenericThreadPool>  pool_{nullptr};

    std::mutex                                          mutex_;
    std::condition_variable                             notEmpty_;
    std::map<JobIdAndTaskId, TaskAndResult>             taskQueue_;
    bool                                                shutdown_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKMANAGER_H_

