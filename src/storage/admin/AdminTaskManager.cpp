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
    if (!shutdown_) {
        std::lock_guard<std::mutex> lk(taskListMutex_);
        taskList_.push_back(task);
        taskListEmpty_.notify_one();
    }
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelTask(int jobId) {
    auto ret = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    {
        std::lock_guard<std::mutex> lk(taskListMutex_);
        for (auto& task : taskList_) {
            if (task->getJobId() == jobId) {
                task->stop();
                ret = kvstore::ResultCode::SUCCEEDED;
            }
        }
    }
    return ret;
}

bool AdminTaskManager::init() {
    bgThread_ = std::make_unique<thread::GenericWorker>();
    shutdown_ = false;
    setSubTaskLimit(FLAGS_subtask_concurrency);
    CHECK(bgThread_->start());
    bgThread_->addTask(&AdminTaskManager::pickTaskThread, this);
    return true;
}

void AdminTaskManager::shutdown() {
    LOG(INFO) << "enter " << __PRETTY_FUNCTION__;
    shutdown_ = true;
    taskListEmpty_.notify_one();
    bgThread_->stop();
    bgThread_->wait();
    LOG(INFO) << "exit " << __PRETTY_FUNCTION__;
}

void AdminTaskManager::pickSubTaskThread() {
    LOG(ERROR) << "enter " << __PRETTY_FUNCTION__;
    auto& currTask = *taskList_.begin();
    size_t idx = subTaskIndex_.fetch_add(1);

    // LOG(ERROR) << "idx=" << idx
    //            << ", subTasks_.size()=" <<  subTasks_.size()
    //            << ", currTask->isStop()=" << currTask->isStop();
    while (idx < subTasks_.size() && !currTask->isStop()) {
        // LOG(ERROR) << "idx=" << idx << " subTasks_.size()=" <<  subTasks_.size();
        subTaskStatus_[idx] = subTasks_[idx].invoke();
        idx = subTaskIndex_.fetch_add(1);
    }
    LOG(ERROR) << "exit " << __PRETTY_FUNCTION__;
}

void AdminTaskManager::runTask(AdminTask& task) {
    LOG(ERROR) << "enter " << __PRETTY_FUNCTION__;
    auto errOrSubTasks = task.genSubTasks();
    if (!nebula::ok(errOrSubTasks)) {
        LOG(ERROR)  << __PRETTY_FUNCTION__ << " generate sub tasks failed ";
        task.finish(nebula::error(errOrSubTasks));
        return;
    }
    // auto& subTasks = nebula::value(errOrSubTasks);
    subTasks_ = nebula::value(errOrSubTasks);

    subTaskStatus_.resize(subTasks_.size(), kvstore::ResultCode::ERR_UNKNOWN);
    size_t concurrency = std::min(subTasks_.size(), (size_t)subTaskLimit_);
    subTaskIndex_ = 0;

    LOG(ERROR) << "use " << concurrency << " thread to run " << subTasks_.size() << " subTasks";
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrency; ++i) {
        threads.emplace_back(std::thread(&AdminTaskManager::pickSubTaskThread, this));
    }

    LOG(ERROR) << "wait for all sub tasks finished";
    for (auto& t : threads) {
        t.join();
    }

    // some task may tolerant some not succeeded code ?
    task.finish(subTaskStatus_);
    LOG(ERROR) << "exit " << __PRETTY_FUNCTION__;
}

void AdminTaskManager::pickTaskThread() {
    while (!shutdown_) {
        {
            std::unique_lock<std::mutex> lk(taskListMutex_);
            LOG(INFO) << "AdminTaskManager::pickTaskThread() waiting for incoming task";
            taskListEmpty_.wait(lk, [&]{ return shutdown_ || !taskList_.empty(); });
        }

        while (!taskList_.empty()) {
            LOG(INFO) << "AdminTaskManager::pickTaskThread() task picked";
            runTask(*taskList_.front());
            std::unique_lock<std::mutex> lk(taskListMutex_);
            taskList_.erase(taskList_.begin());
        }
    }
}

}  // namespace storage
}  // namespace nebula


