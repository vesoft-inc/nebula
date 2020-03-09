/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/CompactTask.h"
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

using ResultCode = nebula::kvstore::ResultCode;

nebula::kvstore::ResultCode CompactTask::run() {
    LOG(ERROR) << "show not call this function";
    return nebula::kvstore::ResultCode::ERR_UNKNOWN;
}

nebula::kvstore::ResultCode CompactTask::stop() {
    LOG(ERROR) << "not implement function " << __PRETTY_FUNCTION__;
    return nebula::kvstore::ResultCode::SUCCEEDED;
}

ErrorOr<ResultCode, std::vector<AdminSubTask>>
CompactTask::genSubTasks() {
    std::vector<AdminSubTask> ret;
    if (!store_) {
        return ret;
    }

    auto errOrSpace = store_->space(spaceId_);
    if (!ok(errOrSpace)) {
        return error(errOrSpace);
    }

    auto space = nebula::value(errOrSpace);

    using FuncObj = std::function<void()>;

    totalSubTasks_ = space->engines_.size();
    for (auto& engine : space->engines_) {
        FuncObj obj = std::bind(&CompactTask::func, this, engine.get());
        ret.emplace_back(obj);
    }
    return ret;
}

void CompactTask::func(nebula::kvstore::KVEngine* engine) {
    auto rc = engine->compact();
    ++finishedSubTasks_;

    if (rc != nebula::kvstore::ResultCode::SUCCEEDED) {
        rc_ = rc;  // what if there are more than one task failed ????
    }

    if (totalSubTasks_ == finishedSubTasks_) {
        onFinished_(rc_);
    }
}

}  // namespace storage
}  // namespace nebula
