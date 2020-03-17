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
AdminTaskFactory::createAdminTask(const cpp2::AddAdminTaskRequest& req,
                                  kvstore::NebulaStore* store,
                                  std::function<void(kvstore::ResultCode)> cb) {
    std::shared_ptr<AdminTask> ret;
    auto cmd = req.get_cmd();
    int jobId = req.get_job_id();
    int taskId = req.get_task_id();
    switch (cmd) {
    case nebula::cpp2::AdminCmd::COMPACT:
        ret.reset(new CompactTask(jobId, taskId, store, req.get_space_id(), cb));
        break;
    case nebula::cpp2::AdminCmd::FLUSH:
        ret.reset(new FlushTask(jobId, taskId, store, req.get_space_id(), cb));
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
