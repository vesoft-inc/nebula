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
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace nebula {
namespace storage {

class AdminTaskManager {
    FRIEND_TEST(TaskManagerTest, happy_path);
    FRIEND_TEST(TaskManagerTest, gen_sub_task_failed);

    using ResultCode = nebula::kvstore::ResultCode;

public:
    struct TaskExecContext {
        explicit TaskExecContext(std::shared_ptr<AdminTask> task) : task_(task) {}
        using SubTaskQueue = folly::UnboundedBlockingQueue<AdminSubTask>;
        using ResultContainer = std::vector<folly::SemiFuture<ResultCode>>;

        std::shared_ptr<AdminTask>  task_{nullptr};
        std::atomic<size_t>         unFinishedTask_{0};
        SubTaskQueue                subtasks_;
    };

    // using ThreadPool = std::unique_ptr<nebula::thread::GenericThreadPool>;
    using ThreadPool = folly::IOThreadPoolExecutor;
    using TaskHandle = std::pair<int, int>;  // jobid + taskid
    using TaskVal = std::shared_ptr<TaskExecContext>;
    using TaskContainer = folly::ConcurrentHashMap<TaskHandle, TaskVal>;
    using TaskQueue = folly::UnboundedBlockingQueue<TaskHandle>;

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

private:
    void pickTaskThread();
    void pickSubTask(TaskHandle handle);

private:
    bool                                    shutdown_{false};
    // ThreadPool                              pool_{nullptr};
    std::unique_ptr<ThreadPool>             pool_{nullptr};
    TaskContainer                           tasks_;
    TaskQueue                               taskQueue_;
    std::unique_ptr<thread::GenericWorker>  bgThread_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKMANAGER_H_

