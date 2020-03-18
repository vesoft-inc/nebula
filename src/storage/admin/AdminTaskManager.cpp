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
    LOG(INFO) << "AdminTaskManager::cancelTask() jobId=" << jobId;
    auto ret = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    {
        std::lock_guard<std::mutex> lk(taskListMutex_);
        for (auto& task : taskList_) {
            if (task->getJobId() == jobId) {
                LOG(INFO) << "task["  << jobId << "], cancelled.";
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

void AdminTaskManager::runTask(AdminTask& task) {
    LOG(INFO) << folly::stringPrintf("runTask(%d, %d)",
                                     task.getJobId(), task.getTaskId());
    auto errOrSubTasks = task.genSubTasks();
    if (!nebula::ok(errOrSubTasks)) {
        LOG(ERROR)  << folly::stringPrintf("task(%d, %d) generate sub tasks failed [%d]",
                                            task.getJobId(), task.getTaskId(),
                                            static_cast<int>(error(errOrSubTasks)));
        task.finish(nebula::error(errOrSubTasks));
        return;
    }

    subTasks_ = nebula::value(errOrSubTasks);

    subTaskStatus_.resize(subTasks_.size(), kvstore::ResultCode::SUCCEEDED);
    size_t concurrency = std::min(subTasks_.size(), (size_t)subTaskLimit_);
    subTaskIndex_ = 0;

    LOG(INFO) << folly::stringPrintf("run %zu sub task in %zu thread",
                                     subTasks_.size(), concurrency);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrency; ++i) {
        threads.emplace_back(std::thread(&AdminTaskManager::pickSubTaskThread, this));
    }
    for (auto& t : threads) {
        t.join();
    }

    // maybe some task will tolerant some err code
    task.finish(subTaskStatus_);
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

void AdminTaskManager::pickSubTaskThread() {
    auto& currTask = *taskList_.begin();
    size_t idx = subTaskIndex_.fetch_add(1);

    while (idx < subTasks_.size()) {
        if (!currTask->isStop()) {
            subTaskStatus_[idx] = subTasks_[idx].invoke();
        } else {
            subTaskStatus_[idx] = kvstore::ResultCode::ERR_USER_CANCELLED;
            LOG(INFO) << "skip cancelled task, sub task =" << idx;
        }
        idx = subTaskIndex_.fetch_add(1);
    }
}

}  // namespace storage
}  // namespace nebula


