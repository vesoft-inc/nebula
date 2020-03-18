/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/FlushTask.h"
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

using ResultCode = nebula::kvstore::ResultCode;

ErrorOr<ResultCode, std::vector<AdminSubTask>>
FlushTask::genSubTasks() {
    std::vector<AdminSubTask> ret;
    if (!store_) {
        return ret;
    }

    auto errOrSpace = store_->space(spaceId_);
    if (!ok(errOrSpace)) {
        return error(errOrSpace);
    }

    auto space = nebula::value(errOrSpace);

    ret.emplace_back([space = space](){
        for (auto& engine : space->engines_) {
            auto code = engine->flush();
            if (code != ResultCode::SUCCEEDED) {
                return code;
            }
        }
        return ResultCode::SUCCEEDED;
    });
    return ret;
}

}  // namespace storage
}  // namespace nebula
