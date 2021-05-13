/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKMANAGER_H_
#define STORAGE_ADMIN_ADMINTASKMANAGER_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <gtest/gtest_prod.h>
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class AdminTaskManager {
    FRIEND_TEST(TaskManagerTest, happy_path);
    FRIEND_TEST(TaskManagerTest, gen_sub_task_failed);

public:
    using ThreadPool = folly::IOThreadPoolExecutor;
    using TaskHandle = std::pair<int, int>;  // jobid + taskid
    using TTask = std::shared_ptr<AdminTask>;
    using TaskContainer = folly::ConcurrentHashMap<TaskHandle, TTask>;
    using TaskQueue = folly::UnboundedBlockingQueue<TaskHandle>;

    AdminTaskManager() = default;
    static AdminTaskManager* instance() {
        static AdminTaskManager sAdminTaskManager;
        return &sAdminTaskManager;
    }

    void addAsyncTask(std::shared_ptr<AdminTask> task);

    void invoke();

    nebula::cpp2::ErrorCode cancelJob(JobID jobId);
    nebula::cpp2::ErrorCode cancelTask(JobID jobId, TaskID taskId = -1);

    bool init();

    void shutdown();

    bool isFinished(JobID jobID, TaskID taskID);

private:
    void schedule();
    void runSubTask(TaskHandle handle);

private:
    bool                                    shutdown_{false};
    std::unique_ptr<ThreadPool>             pool_{nullptr};
    TaskContainer                           tasks_;
    TaskQueue                               taskQueue_;
    std::unique_ptr<thread::GenericWorker>  bgThread_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKMANAGER_H_
