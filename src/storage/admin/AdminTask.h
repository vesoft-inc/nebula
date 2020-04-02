/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASK_H_
#define STORAGE_ADMIN_ADMINTASK_H_

#include "kvstore/Common.h"
#include "common/base/ThriftTypes.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace storage {

class AdminSubTask {
public:
    using ResultCode = nebula::kvstore::ResultCode;
    AdminSubTask() = default;
    explicit AdminSubTask(std::function<ResultCode()> f) : run_(f) {}
    ResultCode invoke() {
        return run_();
    }

private:
    std::function<ResultCode()> run_;
};


enum class TaskPriority : int8_t {
    LO,
    MID,
    HI
};

struct TaskContext {
    using ResultCode = nebula::kvstore::ResultCode;
    using CallBack = std::function<void(ResultCode)>;

    TaskContext() = default;
    TaskContext(const cpp2::AddAdminTaskRequest& req,
                kvstore::NebulaStore* store,
                CallBack cb)
            : cmd_(req.get_cmd())
            , jobId_(req.get_job_id())
            , taskId_(req.get_task_id())
            , spaceId_(req.get_para().get_space_id())
            , store_(store)
            , onFinish_(cb) {}
    ::nebula::cpp2::AdminCmd    cmd_;
    int32_t                     jobId_{-1};
    int32_t                     taskId_{-1};
    int32_t                     spaceId_{-1};
    TaskPriority                pri_{TaskPriority::MID};
    kvstore::NebulaStore*       store_{nullptr};
    CallBack                    onFinish_;
    size_t                      concurrentReq_{INT_MAX};
};

class AdminTask {
    using ResultCode = nebula::kvstore::ResultCode;
    using TCallBack = std::function<void(ResultCode)>;

public:
    AdminTask() = default;
    explicit AdminTask(TaskContext&& ctx) : ctx_(ctx) {}
    virtual ErrorOr<ResultCode, std::vector<AdminSubTask>> genSubTasks() = 0;
    virtual ~AdminTask() {}

    virtual void setCallback(TCallBack cb) {
        ctx_.onFinish_ = cb;
    }

    virtual int8_t getPriority() {
        return static_cast<int8_t>(ctx_.pri_);
    }

    virtual void finish(ResultCode rc) {
        LOG(INFO) << folly::stringPrintf("task(%d, %d) finished, rc=[%d]",
                                         ctx_.jobId_, ctx_.taskId_,
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

    virtual ResultCode Status() const {
        return rc_;
    }

    virtual void subTaskFinish(ResultCode rc) {
        static ResultCode suc{ResultCode::SUCCEEDED};
        rc_.compare_exchange_strong(suc, rc);
    }

    virtual void finish() {
        LOG(INFO) << folly::stringPrintf("task(%d, %d) finished, rc=[%d]",
                     ctx_.jobId_, ctx_.taskId_,
                     static_cast<int>(rc_));
        ctx_.onFinish_(rc_);
    }

    virtual void cancel() {
        FLOG_INFO("task(%d, %d) cancelled", ctx_.jobId_, ctx_.taskId_);
        static ResultCode suc{ResultCode::SUCCEEDED};
        rc_.compare_exchange_strong(suc, ResultCode::ERR_USER_CANCELLED);
    }

protected:
    TaskContext                 ctx_;
    std::atomic<ResultCode>     rc_{ResultCode::SUCCEEDED};
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask>
    createAdminTask(TaskContext&& ctx);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_ADMINTASK_H_
