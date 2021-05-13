/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/CompactTask.h"
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
CompactTask::genSubTasks() {
    std::vector<AdminSubTask> ret;
    if (!env_->kvstore_) {
        return ret;
    }

    auto* store = dynamic_cast<kvstore::NebulaStore*>(env_->kvstore_);
    auto errOrSpace = store->space(*ctx_.parameters_.space_id_ref());
    if (!ok(errOrSpace)) {
        return error(errOrSpace);
    }

    auto space = nebula::value(errOrSpace);

    for (auto& engine : space->engines_) {
        auto task = std::bind(&CompactTask::subTask, this, engine.get());
        ret.emplace_back(task);
    }
    return ret;
}

nebula::cpp2::ErrorCode CompactTask::subTask(kvstore::KVEngine* engine) {
    return engine->compact();
}

}  // namespace storage
}  // namespace nebula
