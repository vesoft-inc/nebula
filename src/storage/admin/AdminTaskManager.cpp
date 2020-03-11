/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/admin/AdminTask.h"
#include "storage/admin/AdminTaskManager.h"

DEFINE_uint32(subtask_concurrency, 10, "The tasks number could be invoked simultaneously");

namespace nebula {
namespace storage {

using nebula::kvstore::ResultCode;
using JobIdAndTaskId = std::pair<int, int>;

void AdminTaskManager::addAsyncTask(std::shared_ptr<AdminTask> task) {
    LOG(ERROR) << "messi enter AdminTaskManager::addAsyncTask2()";
    {
        std::lock_guard<std::mutex> lk(taskListMutex_);
        taskList_.push_back(task);
        taskListEmpty_.notify_one();
    }
    LOG(ERROR) << "messi exit AdminTaskManager::addAsyncTask2()";
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelTask(const cpp2::AddAdminTaskRequest& req) {
    UNUSED(req);
    LOG(FATAL) << "need implement";
    // auto key = std::make_pair(req.get_job_id(), req.get_task_id());
    // {
    //     std::lock_guard<std::mutex> lk(mutex_);
    //     auto iter = taskQueue_.find(key);
    //     if (iter == taskQueue_.begin()) {
    //         auto& taskAndResult = iter->second;
    //         taskAndResult.first->stop();
    //     } else if (iter == taskQueue_.end()) {
    //         return nebula::kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    //     } else {
    //         taskQueue_.erase(iter);
    //     }
    // }
    return nebula::kvstore::ResultCode::SUCCEEDED;
}

bool AdminTaskManager::init() {
    // std::lock_guard<std::mutex> lk(mutex_);
    // pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
    // pool_->start(FLAGS_concurrent_tasks_num);
    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
    bgThread_->addTask(&AdminTaskManager::pickTaskThread, this);
    return true;
}

void AdminTaskManager::shutdown() {
    LOG(INFO) << "enter " << __PRETTY_FUNCTION__;
    shutdown_ = true;
    taskListEmpty_.notify_one();
    // pool_->stop();
    bgThread_->stop();
    bgThread_->wait();
    LOG(INFO) << "exit " << __PRETTY_FUNCTION__;
}

void AdminTaskManager::pickSubTaskThread() {
    auto& currTask = *taskList_.begin();
    size_t idx = subTaskIndex_.fetch_add(1);

    while (idx < subTasks_.size() && !currTask->isStop()) {
        subTaskStatus_[idx] = subTasks_[idx].invoke();
        idx = subTaskIndex_.fetch_add(1);
    }
}

void AdminTaskManager::runTask(AdminTask& task) {
    auto errOrSubTasks = task.genSubTasks();
    if (!nebula::ok(errOrSubTasks)) {
        task.finish(nebula::error(errOrSubTasks));
        return;
    }
    auto& subTasks = nebula::value(errOrSubTasks);
    subTaskStatus_.resize(subTasks.size());
    size_t concurrency = std::min(subTasks.size(),
                                  (size_t)FLAGS_subtask_concurrency);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrency; ++i) {
        threads.emplace_back(std::thread(&AdminTaskManager::pickSubTaskThread, this));
    }

    for (auto& t : threads) {
        t.join();
    }

    // some task may tolerant some not succeeded code ?
    task.finish(subTaskStatus_);
}

void AdminTaskManager::pickTaskThread() {
    while (!shutdown_) {
        std::unique_lock<std::mutex> lk(taskListMutex_);
        LOG(INFO) << "AdminTaskManager::pickTaskThread() waiting for coming task";
        taskListEmpty_.wait(lk, [&]{ return shutdown_ || !taskList_.empty(); });

        if (shutdown_) {
            LOG(INFO) << "AdminTaskManager::pickTaskThread() shutdown called, exit";
            break;
        }

        LOG(INFO) << "AdminTaskManager::pickTaskThread() task picked";
        runTask(**taskList_.begin());

        taskList_.erase(taskList_.begin());
    }
}

}  // namespace storage
}  // namespace nebula


