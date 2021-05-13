/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASK_H_
#define STORAGE_ADMIN_ADMINTASK_H_

#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/thrift/ThriftTypes.h"
#include "kvstore/Common.h"
#include "kvstore/NebulaStore.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

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

enum class TaskPriority : int8_t {
    LO,
    MID,
    HI
};

struct TaskContext {
    using CallBack = std::function<void(nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&)>;

    TaskContext() = default;
    TaskContext(const cpp2::AddAdminTaskRequest& req,
                CallBack cb)
            : cmd_(req.get_cmd())
            , jobId_(req.get_job_id())
            , taskId_(req.get_task_id())
            , parameters_(req.get_para())
            , onFinish_(cb) {}

    nebula::meta::cpp2::AdminCmd    cmd_;
    JobID                           jobId_{-1};
    TaskID                          taskId_{-1};
    nebula::storage::cpp2::TaskPara parameters_;
    TaskPriority                    pri_{TaskPriority::MID};
    CallBack                        onFinish_;
    size_t                          concurrentReq_{INT_MAX};
};

class AdminTask {
    using TCallBack = std::function<void(nebula::cpp2::ErrorCode,
                                         nebula::meta::cpp2::StatisItem&)>;
    using SubTaskQueue = folly::UnboundedBlockingQueue<AdminSubTask>;

public:
    AdminTask() = default;

    explicit AdminTask(StorageEnv* env, TaskContext&& ctx) : env_(env), ctx_(ctx) {}

    virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
    genSubTasks() = 0;

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
        FLOG_INFO("task(%d, %d) finished, rc=[%s]", ctx_.jobId_, ctx_.taskId_,
                  apache::thrift::util::enumNameSafe(rc).c_str());
        nebula::meta::cpp2::StatisItem statisItem;
        ctx_.onFinish_(rc, statisItem);
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

    virtual nebula::cpp2::ErrorCode status() const {
        return rc_;
    }

    virtual void subTaskFinish(nebula::cpp2::ErrorCode rc) {
        auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
        rc_.compare_exchange_strong(suc, rc);
    }

    virtual void cancel() {
        FLOG_INFO("task(%d, %d) cancelled", ctx_.jobId_, ctx_.taskId_);
        auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
        rc_.compare_exchange_strong(suc, nebula::cpp2::ErrorCode::E_USER_CANCEL);
    }

public:
    std::atomic<size_t>         unFinishedSubTask_;
    SubTaskQueue                subtasks_;

protected:
    StorageEnv*                             env_;
    TaskContext                             ctx_;
    std::atomic<nebula::cpp2::ErrorCode>    rc_{nebula::cpp2::ErrorCode::SUCCEEDED};
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask> createAdminTask(StorageEnv* env,
                                                      TaskContext&& ctx);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_ADMINTASK_H_
