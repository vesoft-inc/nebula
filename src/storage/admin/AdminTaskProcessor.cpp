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
    // auto rc = kvstore::ResultCode::SUCCEEDED;
    auto taskManager = AdminTaskManager::instance();
    if (req.get_cmd() == nebula::cpp2::AdminCmd::STOP) {
        setInternalRC(taskManager->cancelTask(req));
        onFinished();
    } else {
        auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
        auto task = AdminTaskFactory::createAdminTask(req, store);
        if (task) {
            auto taskHandle = std::make_pair(req.get_job_id(), req.get_task_id());
            taskManager->addAsyncTask2(taskHandle, task,
                                       [&](nebula::kvstore::ResultCode arc) {
                if (arc != nebula::kvstore::ResultCode::SUCCEEDED) {
                    cpp2::ResultCode thriftRet;
                    thriftRet.set_code(to(arc));
                    codes_.emplace_back(std::move(thriftRet));
                }
                onFinished();
            });
        } else {
            setInternalRC(kvstore::ResultCode::ERR_INVALID_ARGUMENT);
            onFinished();
        }
    }
    // onFinished();
}

}  // namespace storage
}  // namespace nebula


