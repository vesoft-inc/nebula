/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASK_H_
#define STORAGE_ADMIN_ADMINTASK_H_

#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include "storage/admin/TaskUtils.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/Common.h"
#include "kvstore/NebulaStore.h"
#include "thrift/ThriftTypes.h"

namespace nebula {
namespace storage {

class AdminSubTask {
public:
    AdminSubTask() = default;
    explicit AdminSubTask(std::function<cpp2::ErrorCode()> f) : run_(f) {}
    explicit AdminSubTask(std::function<kvstore::ResultCode()> f) {
        run_ = [f = f]() {
            return toStorageErr(f());
        };
    }

    cpp2::ErrorCode invoke() {
        return run_();
    }

private:
    std::function<cpp2::ErrorCode()> run_;
};

enum class TaskPriority : int8_t {
    LO,
    MID,
    HI
};

struct TaskContext {
    using CallBack = std::function<void(cpp2::ErrorCode)>;

    TaskContext() = default;
    TaskContext(const cpp2::AddAdminTaskRequest& req,
                kvstore::KVStore* store,
                CallBack cb)
            : cmd_(req.get_cmd())
            , jobId_(req.get_job_id())
            , taskId_(req.get_task_id())
            , spaceId_(req.get_para().get_space_id())
            , store_(store)
            , onFinish_(cb) {}
    nebula::meta::cpp2::AdminCmd    cmd_;
    int32_t                         jobId_{-1};
    int32_t                         taskId_{-1};
    int32_t                         spaceId_{-1};
    TaskPriority                    pri_{TaskPriority::MID};
    kvstore::KVStore*               store_{nullptr};
    CallBack                        onFinish_;
    size_t                          concurrentReq_{INT_MAX};
};

class AdminTask {
    using TCallBack = std::function<void(cpp2::ErrorCode)>;
    using SubTaskQueue = folly::UnboundedBlockingQueue<AdminSubTask>;

public:
    AdminTask() = default;
    explicit AdminTask(TaskContext&& ctx) : ctx_(ctx) {}
    virtual ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() = 0;
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

    virtual void finish(cpp2::ErrorCode rc) {
        FLOG_INFO("task(%d, %d) finished, rc=[%d]", ctx_.jobId_, ctx_.taskId_,
                  static_cast<int>(rc));
        ctx_.onFinish_(rc);
    }

    virtual int getJobId() {
        return ctx_.jobId_;;
    }

    virtual int getTaskId() {
        return ctx_.taskId_;
    }

    virtual void setConcurrentReq(int concurrenctReq) {
        if (concurrenctReq > 0) {
            ctx_.concurrentReq_ = concurrenctReq;
        }
    }

    virtual size_t getConcurrentReq() {
        return ctx_.concurrentReq_;
    }

    virtual cpp2::ErrorCode status() const {
        return rc_;
    }

    virtual void subTaskFinish(cpp2::ErrorCode rc) {
        static cpp2::ErrorCode suc{cpp2::ErrorCode::SUCCEEDED};
        rc_.compare_exchange_strong(suc, rc);
    }

    virtual void cancel() {
        FLOG_INFO("task(%d, %d) cancelled", ctx_.jobId_, ctx_.taskId_);
        static cpp2::ErrorCode suc{cpp2::ErrorCode::SUCCEEDED};
        rc_.compare_exchange_strong(suc, cpp2::ErrorCode::E_USER_CANCEL);
    }

public:
    std::atomic<size_t>         unFinishedSubTask_;
    SubTaskQueue                subtasks_;

protected:
    TaskContext                     ctx_;
    std::atomic<cpp2::ErrorCode>    rc_{cpp2::ErrorCode::SUCCEEDED};
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask> createAdminTask(TaskContext&& ctx);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_ADMINTASK_H_
