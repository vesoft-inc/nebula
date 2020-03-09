/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/AdminTask.h"
#include "storage/admin/SimpleKVTask.h"
#include "storage/admin/CompactTask.h"

namespace nebula {
namespace storage {

using nebula::kvstore::ResultCode;

std::shared_ptr<AdminTask>
AdminTaskFactory::createAdminTask(const cpp2::AddAdminTaskRequest& req,
                                  kvstore::NebulaStore* store) {
    LOG(ERROR) << "messi " << __PRETTY_FUNCTION__;
    std::shared_ptr<AdminTask> ret;
    auto cmd = req.get_cmd();
    switch (cmd) {
    case nebula::cpp2::AdminCmd::COMPACT:
    case nebula::cpp2::AdminCmd::FLUSH:
        // ret.reset(new SimpleKVTask(cmd, store, req.get_space_id()));
        ret.reset(new CompactTask(cmd, store, req.get_space_id()));
        break;
    case nebula::cpp2::AdminCmd::REBUILD_INDEX:
        break;
    default:
        break;
    }
    LOG(ERROR) << "messi ~" << __PRETTY_FUNCTION__;
    return ret;
}

}  // namespace storage
}  // namespace nebula
