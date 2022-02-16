/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_ADMINTASK_H_
#define STORAGE_ADMIN_ADMINTASK_H_

#include <folly/Optional.h>                                     // for Optional
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>  // for Unbou...
#include <limits.h>                                             // for INT_MAX
#include <stddef.h>                                             // for size_t
#include <stdint.h>                                             // for uint32_t
#include <thrift/lib/cpp/util/EnumUtils.h>                      // for enumN...

#include <algorithm>      // for copy
#include <atomic>         // for atomic
#include <functional>     // for function
#include <memory>         // for share...
#include <string>         // for string
#include <unordered_map>  // for unord...
#include <unordered_set>  // for unord...
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/Base.h"                  // for FLOG_...
#include "common/base/ErrorOr.h"               // for ErrorOr
#include "common/base/Logging.h"               // for LogMe...
#include "common/thrift/ThriftTypes.h"         // for Graph...
#include "interface/gen-cpp2/common_types.h"   // for Error...
#include "interface/gen-cpp2/meta_types.h"     // for AdminCmd
#include "interface/gen-cpp2/storage_types.h"  // for AddTa...
#include "kvstore/Common.h"
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class AdminSubTask {
 public:
  AdminSubTask() = default;

  explicit AdminSubTask(std::function<nebula::cpp2::ErrorCode()> f) : run_(f) {}

  nebula::cpp2::ErrorCode invoke() {
    return run_();
  }

 private:
  std::function<nebula::cpp2::ErrorCode()> run_;
};

enum class TaskPriority : int8_t { LO, MID, HI };

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

class AdminTask {
  using TCallBack = std::function<void(nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatsItem&)>;
  using SubTaskQueue = folly::UnboundedBlockingQueue<AdminSubTask>;

 public:
  AdminTask() = default;

  AdminTask(StorageEnv* env, TaskContext&& ctx) : env_(env), ctx_(ctx) {}

  virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() = 0;

  virtual ~AdminTask() {}

  virtual void setCallback(TCallBack cb) {
    ctx_.onFinish_ = cb;
  }

  virtual int8_t getPriority() {
    return static_cast<int8_t>(ctx_.pri_);
  }

  virtual void finish() {
    finish(rc_);
  }

  virtual void finish(nebula::cpp2::ErrorCode rc) {
    FLOG_INFO("task(%d, %d) finished, rc=[%s]",
              ctx_.jobId_,
              ctx_.taskId_,
              apache::thrift::util::enumNameSafe(rc).c_str());
    running_ = false;
    nebula::meta::cpp2::StatsItem statsItem;
    ctx_.onFinish_(rc, statsItem);
  }

  virtual int getJobId() {
    return ctx_.jobId_;
  }

  virtual int getTaskId() {
    return ctx_.taskId_;
  }

  virtual GraphSpaceID getSpaceId() {
    return ctx_.parameters_.get_space_id();
  }

  virtual void setConcurrentReq(int concurrentReq) {
    if (concurrentReq > 0) {
      ctx_.concurrentReq_ = concurrentReq;
    }
  }

  virtual size_t getConcurrentReq() {
    return ctx_.concurrentReq_;
  }

  virtual nebula::cpp2::ErrorCode status() const {
    return rc_;
  }

  virtual void subTaskFinish(nebula::cpp2::ErrorCode rc) {
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, rc);
  }

  virtual void cancel() {
    FLOG_INFO("task(%d, %d) cancelled", ctx_.jobId_, ctx_.taskId_);
    canceled_ = true;
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, nebula::cpp2::ErrorCode::E_USER_CANCEL);
  }

  virtual bool isRunning() {
    return running_;
  }

  virtual bool isCanceled() {
    return canceled_;
  }

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

class AdminTaskFactory {
 public:
  static std::shared_ptr<AdminTask> createAdminTask(StorageEnv* env, TaskContext&& ctx);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_ADMINTASK_H_
