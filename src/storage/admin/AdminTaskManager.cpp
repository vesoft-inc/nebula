/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/admin/AdminTask.h"
#include "storage/admin/AdminTaskManager.h"

DEFINE_int32(concurrent_tasks_num, 1, "Number of job concurrent tasks num");

namespace nebula {
namespace storage {

using nebula::kvstore::ResultCode;
using JobIdAndTaskId = std::pair<int, int>;

nebula::kvstore::ResultCode
AdminTaskManager::runTaskDirectly(const cpp2::AddAdminTaskRequest& req,
                                  nebula::kvstore::NebulaStore* store) {
    auto spAdminTask = AdminTaskFactory::createAdminTask(req, store);
    return spAdminTask->run();
}

folly::Future<ResultCode>
AdminTaskManager::addAsyncTask(JobIdAndTaskId taskHandle, std::shared_ptr<AdminTask> task) {
    folly::Promise<ResultCode> pro;
    auto fut = pro.getFuture();
    {
        std::lock_guard<std::mutex> lk(mutex_);
        taskQueue_[taskHandle] = std::make_pair(task, std::move(pro));
        notEmpty_.notify_one();
    }
    return fut;
}

void AdminTaskManager::pickTaskThread() {
    while (!shutdown_) {
        std::unique_lock<std::mutex> lk(mutex_);
        LOG(INFO) << "AdminTaskManager::pickTaskThread() waiting for coming task";
        notEmpty_.wait(lk, [&]{ return shutdown_ || !taskQueue_.empty(); });

        if (shutdown_) {
            LOG(INFO) << "AdminTaskManager::pickTaskThread() shutdown called, exit";
            break;
        }
        LOG(INFO) << "AdminTaskManager::pickTaskThread() task picked";
        auto taskAndResult = taskQueue_.begin();
        auto& spAdminTask = taskAndResult->second.first;
        auto& result = taskAndResult->second.second;

        result.setValue(spAdminTask->run());
        taskQueue_.erase(taskQueue_.begin());
    }
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelTask(const cpp2::AddAdminTaskRequest& req) {
    auto key = std::make_pair(req.get_job_id(), req.get_task_id());
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto iter = taskQueue_.find(key);
        if (iter == taskQueue_.begin()) {
            auto& taskAndResult = iter->second;
            taskAndResult.first->stop();
        } else if (iter == taskQueue_.end()) {
            return nebula::kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        } else {
            taskQueue_.erase(iter);
        }
    }
    return nebula::kvstore::ResultCode::SUCCEEDED;
}

bool AdminTaskManager::init() {
    std::lock_guard<std::mutex> lk(mutex_);
    pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
    pool_->start(FLAGS_concurrent_tasks_num);
    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
    bgThread_->addTask(&AdminTaskManager::pickTaskThread, this);
    return true;
}

void AdminTaskManager::shutdown() {
    LOG(INFO) << "enter " << __PRETTY_FUNCTION__;
    shutdown_ = true;
    notEmpty_.notify_one();
    pool_->stop();
    bgThread_->stop();
    bgThread_->wait();
    LOG(INFO) << "exit " << __PRETTY_FUNCTION__;
}

}  // namespace storage
}  // namespace nebula


