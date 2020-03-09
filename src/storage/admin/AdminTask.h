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

struct TaskExecContext {
    explicit TaskExecContext(const cpp2::AddAdminTaskRequest& req) : req_(req) {}
    void setNebulaStore(kvstore::NebulaStore* kvstore) {
        kvstore_ = kvstore;
    }

    cpp2::AddAdminTaskRequest req_;
    kvstore::NebulaStore*     kvstore_{nullptr};
};

class AdminSubTask {
public:
    using ResultCode = nebula::kvstore::ResultCode;
    explicit AdminSubTask(std::function<void()> f) : func_(f) {}
    void invoke() {
        func_();
    }

private:
    std::function<void()> func_;
};

class AdminTask {
    using ResultCode = nebula::kvstore::ResultCode;
public:
    AdminTask() = default;
    virtual ResultCode run() = 0;
    virtual ResultCode stop() = 0;
    virtual ErrorOr<ResultCode, std::vector<AdminSubTask>> genSubTasks() = 0;
    virtual ~AdminTask() {}

    virtual void setCallback(std::function<void(ResultCode)> cb) {
        onFinished_ = cb;
    }

protected:
    std::function<void(ResultCode)>   onFinished_;
    int                     totalSubTasks_{0};
    std::atomic<int32_t>    finishedSubTasks_{0};
    ResultCode              rc_{nebula::kvstore::ResultCode::SUCCEEDED};
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask> createAdminTask(
            const nebula::storage::cpp2::AddAdminTaskRequest& req,
            nebula::kvstore::NebulaStore* store);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASK_H_
