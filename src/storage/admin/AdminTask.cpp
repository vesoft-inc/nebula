/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/AdminTask.h"
#include "storage/admin/CompactTask.h"
#include "storage/admin/FlushTask.h"

namespace nebula {
namespace storage {

using AdminCmd = nebula::meta::cpp2::AdminCmd;

std::shared_ptr<AdminTask>
AdminTaskFactory::createAdminTask(TaskContext&& ctx) {
    FLOG_INFO("%s (%d, %d)", __func__, ctx.jobId_, ctx.taskId_);
    std::shared_ptr<AdminTask> ret;
    switch (ctx.cmd_) {
    case AdminCmd::COMPACT:
        ret = std::make_shared<CompactTask>(std::move(ctx));
        break;
    case AdminCmd::FLUSH:
        ret = std::make_shared<FlushTask>(std::move(ctx));
        break;
    case AdminCmd::REBUILD_TAG_INDEX:
    case AdminCmd::REBUILD_EDGE_INDEX:
        break;
    default:
        break;
    }
    return ret;
}

}  // namespace storage
}  // namespace nebula
