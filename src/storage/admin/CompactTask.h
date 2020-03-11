/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_COMPACTTASK_H_
#define STORAGE_ADMIN_COMPACTTASK_H_

#include "common/base/ThriftTypes.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class CompactTask : public AdminTask {
    using ResultCode = nebula::kvstore::ResultCode;
public:
    CompactTask(int jobId,
                int taskId,
                nebula::kvstore::NebulaStore* store,
                int32_t spaceId,
                std::function<void(kvstore::ResultCode)> cb)
                : AdminTask(jobId, taskId, cb),
                store_(store),
                spaceId_(spaceId) {}

    ErrorOr<ResultCode, std::vector<AdminSubTask>> genSubTasks() override;
    ResultCode subTask(nebula::kvstore::KVEngine* engine);

private:
    // nebula::cpp2::AdminCmd cmd_;
    kvstore::NebulaStore* store_;
    int32_t               spaceId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_COMPACTTASK_H_
