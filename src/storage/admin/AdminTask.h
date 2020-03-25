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
    explicit AdminSubTask(std::function<ResultCode()> f) : run_(f) {}
    ResultCode invoke() {
        return run_();
    }

private:
    std::function<ResultCode()> run_;
};

class AdminTask {
    using ResultCode = nebula::kvstore::ResultCode;
    using TCallBack = std::function<void(ResultCode)>;

public:
    enum class TaskPriority : int8_t {
        LOW,
        MID,
        HIGH
    };

    AdminTask() = default;
    AdminTask(int jobId, int taskId, TCallBack cb) :
              jobId_(jobId),
              taskId_(taskId),
              onFinished_(cb) {}
    virtual ErrorOr<ResultCode, std::vector<AdminSubTask>> genSubTasks() = 0;
    virtual ~AdminTask() {}

    virtual void setCallback(TCallBack cb) {
        onFinished_ = cb;
    }

    virtual void cancel() {
        stop_ = true;
        if (rc_ == ResultCode::SUCCEEDED) {
            rc_ = ResultCode::ERR_USER_CANCELLED;
        }
    }

    virtual bool cancelled() {
        return stop_;
    }

    virtual int8_t getPriority() {
        return static_cast<int8_t>(pri_);
    }

    virtual int subFinish(ResultCode rc) {
        if (rc_ == ResultCode::SUCCEEDED) {
            rc_ = rc;
        }
        return --notFinishedSubtask_;
    }

    virtual void finish() {
        finish(rc_);
    }

    virtual void finish(ResultCode rc) {
        LOG(INFO) << folly::stringPrintf("job[%d], task[%d] finished, rc=[%d]",
                                         jobId_, taskId_, static_cast<int>(rc));
        if (rc_ == ResultCode::SUCCEEDED) {
            rc_ = rc;
        }
        onFinished_(rc_);
    }

    virtual void finish(const std::vector<ResultCode>& subTaskResults) {
        ResultCode rc = ResultCode::SUCCEEDED;
        for (auto& _t : subTaskResults) {
            if (ResultCode::SUCCEEDED != _t) {
                rc = _t;
                break;
            }
        }
        finish(rc);
    }

    virtual int getJobId() {
        return jobId_;
    }

    virtual int getTaskId() {
        return taskId_;
    }

    virtual void setConcurrent(int concurrcy) {
        concurrency_ = std::max(1, concurrcy);
    }

    virtual int getConcurrent() {
        return concurrency_;
    }

protected:
    int             jobId_{-1};
    int             taskId_{-1};
    TCallBack       onFinished_;
    bool            stop_{false};
    TaskPriority    pri_{TaskPriority::MID};
    int             concurrency_{INT_MAX};
    int             notFinishedSubtask_{0};
    int             finishedSubtasks_{0};
    ResultCode      rc_{ResultCode::SUCCEEDED};
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask> createAdminTask(
            const nebula::storage::cpp2::AddAdminTaskRequest& req,
            nebula::kvstore::NebulaStore* store,
            std::function<void(nebula::kvstore::ResultCode)> cb);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASK_H_
