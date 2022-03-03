/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_ADMINTASK_H_
#define STORAGE_ADMIN_ADMINTASK_H_

#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/meta_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/Common.h"
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

/**
 * @brief Subtask class for admin tasks. An admin task comprises a sequence of subtasks.
 *
 */
class AdminSubTask {
 public:
  AdminSubTask() = default;

  explicit AdminSubTask(std::function<nebula::cpp2::ErrorCode()> f) : run_(f) {}

  /**
   * @brief Entry point to invoke sub tasks function.
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode invoke() {
    return run_();
  }

 private:
  std::function<nebula::cpp2::ErrorCode()> run_;
};

enum class TaskPriority : int8_t { LO, MID, HI };

/**
 * @brief Task context for execution.
 *
 */
struct TaskContext {
  using CallBack = std::function<void(nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&)>;

  TaskContext() = default;
  TaskContext(const cpp2::AddTaskRequest& req, CallBack cb)
      : cmd_(req.get_cmd()),
        jobId_(req.get_job_id()),
        taskId_(req.get_task_id()),
        parameters_(req.get_para()),
        onFinish_(cb) {}

  nebula::meta::cpp2::AdminCmd cmd_;
  JobID jobId_{-1};
  TaskID taskId_{-1};
  nebula::storage::cpp2::TaskPara parameters_;
  TaskPriority pri_{TaskPriority::MID};
  CallBack onFinish_;
  size_t concurrentReq_{INT_MAX};
};

/**
 * @brief Admin task base class. Define admin task base interface.
 *
 */
class AdminTask {
  using TCallBack = std::function<void(nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&)>;
  using SubTaskQueue = folly::UnboundedBlockingQueue<AdminSubTask>;

 public:
  AdminTask() = default;

  AdminTask(StorageEnv* env, TaskContext&& ctx) : env_(env), ctx_(ctx) {}

  /**
   * @brief Generate subtasks for admin jobs.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() = 0;

  virtual ~AdminTask() {}

  /**
   * @brief Set the Callback object
   *
   * @param cb Callback should be called while the task is finished.
   */
  virtual void setCallback(TCallBack cb) {
    ctx_.onFinish_ = cb;
  }

  /**
   * @brief Get the Priority
   *
   * @return int8_t Priority.
   */
  virtual int8_t getPriority() {
    return static_cast<int8_t>(ctx_.pri_);
  }

  /**
   * @brief Handling after the task is finished.
   *
   */
  virtual void finish() {
    finish(rc_);
  }

  /**
   * @brief Handling after the task is finished by return code.
   *
   * @param rc
   */
  virtual void finish(nebula::cpp2::ErrorCode rc) {
    FLOG_INFO("task(%d, %d) finished, rc=[%s]",
              ctx_.jobId_,
              ctx_.taskId_,
              apache::thrift::util::enumNameSafe(rc).c_str());
    running_ = false;
    nebula::meta::cpp2::StatsItem statsItem;
    ctx_.onFinish_(rc, statsItem);
  }

  /**
   * @brief Get the Job Id
   *
   * @return int Job id.
   */
  virtual int getJobId() {
    return ctx_.jobId_;
  }

  /**
   * @brief Get the Task Id
   *
   * @return int Task id.
   */
  virtual int getTaskId() {
    return ctx_.taskId_;
  }

  /**
   * @brief Get the Space Id
   *
   * @return GraphSpaceID Space id.
   */
  virtual GraphSpaceID getSpaceId() {
    return ctx_.parameters_.get_space_id();
  }

  /**
   * @brief Set the Concurrent Request
   *
   * @param concurrentReq Number of concurrent requests.
   */
  virtual void setConcurrentReq(int concurrentReq) {
    if (concurrentReq > 0) {
      ctx_.concurrentReq_ = concurrentReq;
    }
  }

  /**
   * @brief Get the Concurrent Requests number.
   *
   * @return size_t Concurrent requests number.
   */
  virtual size_t getConcurrentReq() {
    return ctx_.concurrentReq_;
  }

  /**
   * @brief Get error code.
   *
   * @return nebula::cpp2::ErrorCode Errorcode.
   */
  virtual nebula::cpp2::ErrorCode status() const {
    return rc_;
  }

  /**
   * @brief Set task finished with return code.
   *
   * @param rc Errorcode.
   */
  virtual void subTaskFinish(nebula::cpp2::ErrorCode rc) {
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, rc);
  }

  /**
   * @brief Cancel task.
   *
   */
  virtual void cancel() {
    FLOG_INFO("task(%d, %d) cancelled", ctx_.jobId_, ctx_.taskId_);
    canceled_ = true;
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, nebula::cpp2::ErrorCode::E_USER_CANCEL);
  }

  /**
   * @brief Check if task is  running.
   *
   * @return true Task is running.
   * @return false Task is not running.
   */
  virtual bool isRunning() {
    return running_;
  }

  /**
   * @brief Check if task is Canceled.
   *
   * @return true Task is Canceled.
   * @return false Task is not Canceled.
   */
  virtual bool isCanceled() {
    return canceled_;
  }

  /**
   * @brief Get admin job's command type.
   *
   * @return meta::cpp2::AdminCmd job's command type.
   */
  meta::cpp2::AdminCmd cmdType() {
    return ctx_.cmd_;
  }

 public:
  std::atomic<size_t> unFinishedSubTask_;
  SubTaskQueue subtasks_;
  std::atomic<bool> running_{false};

 protected:
  StorageEnv* env_;
  TaskContext ctx_;
  std::atomic<nebula::cpp2::ErrorCode> rc_{nebula::cpp2::ErrorCode::SUCCEEDED};
  std::atomic<bool> canceled_{false};
};

/**
 * @brief Factory class to create admin tasks.
 *
 */
class AdminTaskFactory {
 public:
  static std::shared_ptr<AdminTask> createAdminTask(StorageEnv* env, TaskContext&& ctx);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_ADMINTASK_H_
