/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/CompactTask.h"
#include "storage/admin/TaskUtils.h"
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>>
CompactTask::genSubTasks() {
    std::vector<AdminSubTask> ret;
    if (!env_->kvstore_) {
        return ret;
    }

    auto* store = dynamic_cast<kvstore::NebulaStore*>(env_->kvstore_);
    auto errOrSpace = store->space(ctx_.parameters_.space_id);
    if (!ok(errOrSpace)) {
        return toStorageErr(error(errOrSpace));
    }

    auto space = nebula::value(errOrSpace);

    using FuncObj = std::function<kvstore::ResultCode()>;
    for (auto& engine : space->engines_) {
        FuncObj obj = std::bind(&CompactTask::subTask, this, engine.get());
        ret.emplace_back(obj);
    }
    return ret;
}

kvstore::ResultCode CompactTask::subTask(kvstore::KVEngine* engine) {
    return engine->compact();
}

}  // namespace storage
}  // namespace nebula
