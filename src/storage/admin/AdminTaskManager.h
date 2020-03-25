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

namespace nebula {
namespace storage {


class AdminTaskManager {
    FRIEND_TEST(TaskManagerTest, happy_path);
    FRIEND_TEST(TaskManagerTest, gen_sub_task_failed);

public:
    struct TaskExecContext {
        explicit TaskExecContext(std::shared_ptr<AdminTask> task_) : task(task_) {}
        std::shared_ptr<AdminTask>  task{nullptr};
        std::vector<AdminSubTask>   subTasks;
        std::atomic<int>            finishedsubTasks{0};
    };

    using ResultCode = nebula::kvstore::ResultCode;
    using ASyncTaskRet = folly::Promise<ResultCode>;
    using TaskAndResult = std::pair<std::shared_ptr<AdminTask>, ASyncTaskRet>;
    using FutureResultCode = folly::Future<ResultCode>;

    using TaskHandle = std::pair<int, int>;         // jobid + taskid
    using SubTaskHandle = std::tuple<int, int, int>;    // jobid / taskid / subtaskid
    using TaskQueue = folly::UnboundedBlockingQueue<TaskHandle>;
    using SubTaskQueue = folly::UnboundedBlockingQueue<SubTaskHandle>;
    using TaskVal = std::shared_ptr<TaskExecContext>;
    using TaskContainer = folly::ConcurrentHashMap<TaskHandle, TaskVal>;

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

    void setSubTaskLimit(int limit) { activeWorker_ = limit; }

private:
    void pickTaskThread();
    void pickSubTaskThread(int threadIndex);
    bool satisifyConcurrency(int concurrentReqNum);

private:
    std::unique_ptr<thread::GenericWorker> bgThread_;
    std::unique_ptr<nebula::thread::GenericThreadPool>  pool_{nullptr};

    bool                                            shutdown_{true};

    TaskContainer                                   tasks_;
    TaskQueue                                       taskHandleQueue_;
    SubTaskQueue                                    subTaskHandleQueue_;
    std::mutex                                      muConLimits_;
    std::multiset<int>                              concurrentLimits;
    std::atomic<int>                                activeWorker_{0};
    int                                             maxWorker_{0};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKMANAGER_H_

