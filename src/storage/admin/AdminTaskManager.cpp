/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/admin/AdminTask.h"
#include "storage/admin/AdminTaskManager.h"

DEFINE_uint32(max_task_concurrency, 10, "The tasks number could be invoked simultaneously");
DEFINE_uint32(max_concurrent_subtasks, 10, "The sub tasks could be invoked simultaneously");

namespace nebula {
namespace storage {

using ResultCode = nebula::kvstore::ResultCode;
using TaskHandle = std::pair<int, int>;     // jobid + taskid

bool AdminTaskManager::init() {
    LOG(INFO) << "max concurrenct subtasks: " << FLAGS_max_concurrent_subtasks;
    pool_ = std::make_unique<ThreadPool>(FLAGS_max_concurrent_subtasks);

    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
    bgThread_->addTask(&AdminTaskManager::schedule, this);

    shutdown_ = false;
    LOG(INFO) << "exit AdminTaskManager::init()";
    return true;
}

void AdminTaskManager::addAsyncTask(std::shared_ptr<AdminTask> task) {
    TaskHandle handle = std::make_pair(task->getJobId(), task->getTaskId());
    LOG(INFO) << folly::stringPrintf("try enqueue task(%d, %d), con req=%zu",
                                     task->getJobId(), task->getTaskId(),
                                     task->getConcurrentReq());
    auto ctx = std::make_shared<TaskExecContext>(task);
    tasks_.insert(handle, ctx);
    taskQueue_.add(handle);
    LOG(INFO) << folly::stringPrintf("enqueue task(%d, %d), con req=%zu",
                                     task->getJobId(), task->getTaskId(),
                                     task->getConcurrentReq());
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelJob(int jobId) {
    auto ret = ResultCode::ERR_KEY_NOT_FOUND;
    auto it = tasks_.begin();
    while (it != tasks_.end()) {
        auto handle = it->first;
        if (handle.first == jobId) {
            auto ctx = it->second;
            ctx->task_->cancel();
            FLOG_INFO("task(%d, %d) cancelled", jobId, handle.second);
            ret = kvstore::ResultCode::SUCCEEDED;
        }
        ++it;
    }
    return ret;
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelTask(int jobId, int taskId) {
    if (taskId < 0) {
        return cancelJob(jobId);
    }
    auto ret = ResultCode::SUCCEEDED;
    TaskHandle handle = std::make_pair(jobId, taskId);
    auto it = tasks_.find(handle);
    if (it == tasks_.cend()) {
        ret = ResultCode::ERR_KEY_NOT_FOUND;
    } else {
        it->second->task_->cancel();
    }
    return ret;
}

void AdminTaskManager::shutdown() {
    LOG(INFO) << "enter AdminTaskManager::shutdown()";
    shutdown_ = true;
    bgThread_->stop();
    bgThread_->wait();

    for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
        it->second->task_->cancel();  // cancelled_ = true;
    }

    pool_->join();

    LOG(INFO) << "exit AdminTaskManager::shutdown()";
}

// schedule
void AdminTaskManager::schedule() {
    std::chrono::milliseconds interval{20};    // 20ms
    while (!shutdown_) {
        LOG(INFO) << "waiting for incoming task";
        folly::Optional<TaskHandle> optTaskHandle{folly::none};
        while (!optTaskHandle && !shutdown_) {
            optTaskHandle = taskQueue_.try_take_for(interval);
        }

        if (shutdown_) {
            LOG(INFO) << "detect AdminTaskManager::shutdown()";
            return;
        }

        auto handle = *optTaskHandle;
        LOG(INFO) << folly::stringPrintf("dequeue task(%d, %d)",
                                         handle.first, handle.second);
        auto it = tasks_.find(handle);
        if (it == tasks_.end()) {
            LOG(ERROR) << folly::stringPrintf(
                        "trying to exec non-exist task(%d, %d)",
                        handle.first, handle.second);
            continue;
        }

        std::shared_ptr<TaskExecContext> ctx = it->second;
        auto errOrSubTasks = ctx->task_->genSubTasks();
        if (!nebula::ok(errOrSubTasks)) {
            LOG(ERROR) << "genSubTasks() failed=" << static_cast<int>(nebula::error(errOrSubTasks));
            ctx->task_->finish(nebula::error(errOrSubTasks));
            tasks_.erase(handle);
            return;
        }

        auto subTasks = nebula::value(errOrSubTasks);
        for (auto& subtask : subTasks) {
            ctx->subtasks_.add(subtask);
        }

        auto subTaskConcurrency = std::min(ctx->task_->getConcurrentReq(),
                                           static_cast<size_t>(FLAGS_max_concurrent_subtasks));
        subTaskConcurrency = std::min(subTaskConcurrency, subTasks.size());
        ctx->unFinishedTask_ = subTasks.size();

        LOG(INFO) << folly::stringPrintf("run task(%d, %d), %zu subtasks in %zu thread",
                                         handle.first, handle.second,
                                         ctx->unFinishedTask_,
                                         subTaskConcurrency);
        for (size_t i = 0; i < subTaskConcurrency; ++i) {
            pool_->add(std::bind(&AdminTaskManager::runSubTask, this, handle));
        }
    }  // end while (!shutdown_)
    LOG(INFO) << "AdminTaskManager::pickTaskThread(~)";
}

void AdminTaskManager::runSubTask(TaskHandle handle) {
    auto it = tasks_.find(handle);
    if (it == tasks_.cend()) {
        FLOG_INFO("task(%d, %d) runSubTask() exit", handle.first, handle.second);
        return;
    }
    auto ctx = it->second;
    std::chrono::milliseconds take_dura{10};
    if (auto subTask = ctx->subtasks_.try_take_for(take_dura)) {
        // LOG(INFO) << "take sub task";
        if (ctx->task_->Status() == ResultCode::SUCCEEDED) {
            ResultCode rc = subTask->invoke();
            ctx->task_->subTaskFinish(rc);
        }

        {
            std::lock_guard<std::mutex> lk(ctx->mu_);
            if (0 == --ctx->unFinishedTask_) {
                FLOG_INFO("task(%d, %d) finished", ctx->task_->getJobId(),
                                                   ctx->task_->getTaskId());
                ctx->task_->finish();
                tasks_.erase(handle);
            } else {
                pool_->add(std::bind(&AdminTaskManager::runSubTask, this, handle));
            }
        }
    } else {
        FLOG_INFO("task(%d, %d) runSubTask() exit", handle.first, handle.second);
    }
}

}  // namespace storage
}  // namespace nebula
