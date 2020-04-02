/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/AdminTask.h"
#include "storage/admin/CompactTask.h"
#include "storage/admin/FlushTask.h"

namespace nebula {
namespace storage {

std::shared_ptr<AdminTask>
AdminTaskFactory::createAdminTask(TaskContext&& ctx) {
    LOG(INFO) << folly::stringPrintf("%s (%d, %d)",
                                    __func__, ctx.jobId_, ctx.taskId_);
    std::shared_ptr<AdminTask> ret;
    switch (ctx.cmd_) {
    case nebula::cpp2::AdminCmd::COMPACT:
        ret.reset(new CompactTask(std::move(ctx)));
        break;
    case nebula::cpp2::AdminCmd::FLUSH:
        ret.reset(new FlushTask(std::move(ctx)));
        break;
    case nebula::cpp2::AdminCmd::REBUILD_TAG_INDEX:
    case nebula::cpp2::AdminCmd::REBUILD_EDGE_INDEX:
        break;
    default:
        break;
    }
    return ret;
}

}  // namespace storage
}  // namespace nebula
