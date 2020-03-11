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
    AdminTask() = default;
    explicit AdminTask(TCallBack cb) : onFinished_(cb) {}
    virtual ErrorOr<ResultCode, std::vector<AdminSubTask>> genSubTasks() = 0;
    virtual ~AdminTask() {}

    virtual void setCallback(TCallBack cb) {
        onFinished_ = cb;
    }

    virtual void stop() {
        stop_ = true;
    }

    virtual bool isStop() {
        return stop_;
    }

    virtual void finish(ResultCode rc) {
        onFinished_(rc);
    }

    virtual void finish(const std::vector<ResultCode>& subTaskResults) {
        ResultCode rc = ResultCode::SUCCEEDED;
        for (auto _t : subTaskResults) {
            if (ResultCode::SUCCEEDED != _t) {
                rc = _t;
                break;
            }
        }
        onFinished_(rc);
    }

protected:
    TCallBack     onFinished_;
    bool          stop_{false};
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask> createAdminTask(
            const nebula::storage::cpp2::AddAdminTaskRequest& req,
            nebula::kvstore::NebulaStore* store,
            std::function<void(nebula::kvstore::ResultCode)> cb);
};

// struct TaskExecContext {
//     explicit TaskExecContext(const cpp2::AddAdminTaskRequest& req) : req_(req) {}
//     void setNebulaStore(kvstore::NebulaStore* kvstore) {
//         kvstore_ = kvstore;
//     }

//     cpp2::AddAdminTaskRequest req_;
//     kvstore::NebulaStore*     kvstore_{nullptr};
// };

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASK_H_
