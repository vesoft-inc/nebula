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
#include <gtest/gtest_prod.h>
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class AdminTaskManager {
    FRIEND_TEST(TaskManagerTest, happy_path);
    FRIEND_TEST(TaskManagerTest, gen_sub_task_failed);

    using ResultCode = nebula::kvstore::ResultCode;
    using ASyncTaskRet = folly::Promise<ResultCode>;
    using TaskAndResult = std::pair<std::shared_ptr<AdminTask>, ASyncTaskRet>;
    using JobIdAndTaskId = std::pair<int, int>;
    using FutureResultCode = folly::Future<ResultCode>;

public:
    AdminTaskManager() = default;
    static AdminTaskManager* instance() {
        static AdminTaskManager sAdminTaskManager;
        return &sAdminTaskManager;
    }

    void addAsyncTask(std::shared_ptr<AdminTask> task);

    void invoke();

    ResultCode cancelTask(int jobId);

    bool init();

    void shutdown();

    void setSubTaskLimit(int limit) { subTaskLimit_ = limit; }

private:
    void runTask(AdminTask& task);
    void pickTaskThread();
    void pickSubTaskThread();

private:
    std::unique_ptr<thread::GenericWorker> bgThread_;
    std::unique_ptr<nebula::thread::GenericThreadPool>  pool_{nullptr};

    int                                             subTaskLimit_{1};
    bool                                            shutdown_;

    std::deque<std::shared_ptr<AdminTask>>          taskList_;
    std::mutex                                      taskListMutex_;
    std::condition_variable                         taskListEmpty_;

    std::vector<AdminSubTask>                       subTasks_;
    std::vector<ResultCode>                         subTaskStatus_;
    std::atomic<int>                                subTaskIndex_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKMANAGER_H_

