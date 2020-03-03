/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/AdminTaskProcessor.h"
#include "storage/admin/AdminTaskManager.h"
#include "gen-cpp2/common_types.h"

namespace nebula {
namespace storage {

void AdminTaskProcessor::process(const cpp2::AddAdminTaskRequest& req) {
    auto rc = nebula::kvstore::ResultCode::SUCCEEDED;
    auto& taskManager = AdminTaskManager::instance();
    if (req.get_cmd() == nebula::cpp2::AdminCmd::STOP) {
        rc = taskManager.cancelTask(req);
    } else {
        auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
        auto fut = taskManager.addAsyncTask(req, store);
        fut.wait();
    }
    if (rc != kvstore::ResultCode::SUCCEEDED) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(to(rc));
        codes_.emplace_back(std::move(thriftRet));
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula


