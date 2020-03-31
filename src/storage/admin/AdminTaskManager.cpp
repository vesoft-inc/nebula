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
    pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
    CHECK(pool_->start(FLAGS_max_concurrent_subtasks));

    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
    bgThread_->addTask(&AdminTaskManager::pickTaskThread, this);

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
AdminTaskManager::cancelTask(int jobId) {
    LOG(INFO) << "AdminTaskManager::cancelTask() jobId=" << jobId;
    auto ret = ResultCode::ERR_KEY_NOT_FOUND;
    auto it = tasks_.begin();
    while (it != tasks_.end()) {
        auto handle = it->first;
        if (handle.first == jobId) {
            auto ctx = it->second;
            ctx->cancelled_ = true;
            ctx->rc_ = ResultCode::ERR_USER_CANCELLED;
            LOG(INFO) << folly::stringPrintf("task(%d, %d) cancelled",
                                             jobId, handle.second);
            ret = kvstore::ResultCode::SUCCEEDED;
        }
        ++it;
    }
    return ret;
}

void AdminTaskManager::shutdown() {
    LOG(INFO) << "enter AdminTaskManager::shutdown()";
    shutdown_ = true;
    bgThread_->stop();
    bgThread_->wait();

    for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
        it->second->cancelled_ = true;
    }

    pool_->stop();
    pool_->wait();

    bgThread_->stop();
    bgThread_->wait();

    LOG(INFO) << "exit AdminTaskManager::shutdown()";
}

void AdminTaskManager::pickTaskThread() {
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
        ctx->unFinishedThread_ = subTaskConcurrency;

        LOG(INFO) << folly::stringPrintf("run task(%d, %d) in %zu thread",
                                         handle.first, handle.second,
                                         ctx->unFinishedTask_.load());
        for (size_t i = 0; i < subTaskConcurrency; ++i) {
            pool_->addTask(&AdminTaskManager::pickSubTask, this, handle);
        }
    }  // end while (!shutdown_)
    LOG(INFO) << "AdminTaskManager::pickTaskThread(~)";
}

void AdminTaskManager::pickSubTask(TaskHandle handle) {
    std::chrono::milliseconds take_dura{10};
    ResultCode success{ResultCode::SUCCEEDED};
    auto ctx = tasks_[handle];
    while (auto subTask = ctx->subtasks_.try_take_for(take_dura)) {
        if (!ctx->cancelled_ && ctx->rc_ == ResultCode::SUCCEEDED) {
            ResultCode rc = subTask->invoke();
            if (rc != ResultCode::SUCCEEDED) {
                ctx->rc_.compare_exchange_strong(success, rc);
            }
        }
        --ctx->unFinishedTask_;
    }

    {
        std::unique_lock<std::mutex> lk(ctx->mutex_);
        // report status to upper caller ASAP
        if (ctx->unFinishedTask_ == 0 && !ctx->onFinishCalled) {
            ctx->task_->finish(ctx->rc_);
            ctx->onFinishCalled = true;
        }

        if (ctx->unFinishedThread_ > 1) {
            --ctx->unFinishedThread_;
        } else {  // the last worker do the clean up
            tasks_.erase(handle);
        }
    }
}

}  // namespace storage
}  // namespace nebula
