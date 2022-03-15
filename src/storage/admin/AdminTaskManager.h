/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_ADMINTASKMANAGER_H_
#define STORAGE_ADMIN_ADMINTASKMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <gtest/gtest_prod.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Class to manage admin task status.
 *
 */
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
  explicit AdminTaskManager(storage::StorageEnv* env = nullptr) : env_(env) {}
  /**
   * @brief Construct new instance of AdminTaskManager
   *
   * @param env Related environment variables for storage.
   * @return AdminTaskManager* AdminTaskManager instance.
   */
  static AdminTaskManager* instance(storage::StorageEnv* env = nullptr) {
    static AdminTaskManager sAdminTaskManager(env);
    return &sAdminTaskManager;
  }

  ~AdminTaskManager() {
    if (metaClient_ != nullptr) {
      metaClient_ = nullptr;
    }
  }

  // Caller must make sure JobId + TaskId is unique
  void addAsyncTask(std::shared_ptr<AdminTask> task);

  void invoke();

  nebula::cpp2::ErrorCode cancelJob(JobID jobId);
  nebula::cpp2::ErrorCode cancelTask(JobID jobId, TaskID taskId = -1);

  void cancelTasks(GraphSpaceID spaceId);
  int32_t runningTaskCnt(GraphSpaceID spaceId);
  void waitCancelTasks(GraphSpaceID spaceId);

  bool init();

  void shutdown();

  bool isFinished(JobID jobID, TaskID taskID);

  void saveTaskStatus(JobID jobId,
                      TaskID taskId,
                      nebula::cpp2::ErrorCode rc,
                      const nebula::meta::cpp2::StatsItem& result);

  void removeTaskStatus(JobID jobId, TaskID taskId);

  void handleUnreportedTasks();

  void notifyReporting();

  void saveAndNotify(JobID jobId,
                     TaskID taskId,
                     nebula::cpp2::ErrorCode rc,
                     const nebula::meta::cpp2::StatsItem& result);

 protected:
  meta::MetaClient* metaClient_{nullptr};

 private:
  /**
   * @brief Schedule tasks in the queue
   *
   */
  void schedule();

  void runSubTask(TaskHandle handle);

 private:
  std::atomic<bool> shutdown_{false};
  std::unique_ptr<ThreadPool> pool_{nullptr};
  TaskContainer tasks_;
  TaskQueue taskQueue_;
  std::unique_ptr<thread::GenericWorker> bgThread_;
  storage::StorageEnv* env_{nullptr};
  std::unique_ptr<std::thread> unreportedAdminThread_;
  std::mutex unreportedMutex_;
  std::condition_variable unreportedCV_;
  bool ifAnyUnreported_{true};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKMANAGER_H_
