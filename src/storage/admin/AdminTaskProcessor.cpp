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
    bool runDirectly = true;
    auto rc = nebula::kvstore::ResultCode::SUCCEEDED;
    auto taskManager = AdminTaskManager::instance();

    auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
    auto cb = [&](nebula::kvstore::ResultCode ret) {
        if (ret != nebula::kvstore::ResultCode::SUCCEEDED) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(to(ret));
            codes_.emplace_back(std::move(thriftRet));
        }
        onFinished();
    };
    TaskContext ctx(req, store, cb);
    auto task = AdminTaskFactory::createAdminTask2(std::move(ctx));
    if (task) {
        runDirectly = false;
        taskManager->addAsyncTask(task);
    } else {
        rc = kvstore::ResultCode::ERR_INVALID_ARGUMENT;
    }

    if (runDirectly) {
        if (rc != kvstore::ResultCode::SUCCEEDED) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(to(rc));
            codes_.emplace_back(std::move(thriftRet));
        }
        onFinished();
    }
}

}  // namespace storage
}  // namespace nebula


