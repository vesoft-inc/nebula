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
    LOG(INFO) << "messi " << __PRETTY_FUNCTION__;
    UNUSED(req);
    auto& taskManager = AdminTaskManager::instance();
    if (req.get_cmd() == nebula::cpp2::AdminCmd::STOP) {
        taskManager.cancelTask(std::move(req));
    } else {
        taskManager.addTask(std::move(req));
    }
    // auto spaceId = req.get_space_id();
    // auto& name = req.get_name();
    // auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
    // auto retCode = store->createCheckpoint(spaceId, std::move(name));
    // if (retCode != kvstore::ResultCode::SUCCEEDED) {
    //     cpp2::ResultCode thriftRet;
    //     thriftRet.set_code(to(retCode));
    //     codes_.emplace_back(std::move(thriftRet));
    // }
    onFinished();
}

}  // namespace storage
}  // namespace nebula


