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

nebula::kvstore::ResultCode
AdminTaskManager::runTaskDirectly(const cpp2::AddAdminTaskRequest& req,
                                  nebula::kvstore::NebulaStore* store) {
    auto spAdminTask = AdminTaskFactory::createAdminTask(req, store);
    return spAdminTask->run();
}

// JobManager can guarantee there won't be more than 1 task
nebula::kvstore::ResultCode
AdminTaskManager::addAsyncTask(const cpp2::AddAdminTaskRequest& req,
                               nebula::kvstore::NebulaStore* store) {
    auto key = std::make_pair(req.get_job_id(), req.get_task_id());
    auto spAdminTask = AdminTaskFactory::createAdminTask(req, store);
    if (!spAdminTask) {
        return nebula::kvstore::ResultCode::ERR_INVALID_ARGUMENT;
    }
    {
        std::lock_guard<std::mutex> lk(mutex_);
        inQueueTasks_[key] = spAdminTask;
        notEmpty_.notify_one();
    }
    return nebula::kvstore::ResultCode::SUCCEEDED;
}

void AdminTaskManager::pickTaskThread() {
    while (!shutdown_) {
        std::unique_lock<std::mutex> lk(mutex_);
        notEmpty_.wait(lk);
        // notEmpty_.wait(lk, [&]{ return !inQueueTasks_.empty(); });
        // while (inQueueTasks_.empty()) {
        //     notEmpty_.wait(lk, []{return !inQueueTasks_.empty()});
        // }
        auto firstTask = inQueueTasks_.begin();
        // auto rc = firstTask->second->run();
        firstTask->second->run();
        inQueueTasks_.erase(firstTask);
    }
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelTask(const cpp2::AddAdminTaskRequest& req) {
    auto key = std::make_pair(req.get_job_id(), req.get_task_id());
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto iter = inQueueTasks_.find(key);
        if (iter == inQueueTasks_.begin()) {
            iter->second->stop();
        } else if (iter == inQueueTasks_.end()) {
            return nebula::kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        } else {
            inQueueTasks_.erase(iter);
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
    pool_->stop();
    bgThread_->stop();
    bgThread_->wait();
    LOG(INFO) << "exit " << __PRETTY_FUNCTION__;
}

}  // namespace storage
}  // namespace nebula


