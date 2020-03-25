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

using ResultCode = nebula::kvstore::ResultCode;
using TaskHandle = std::pair<int, int>;     // jobid + taskid

bool AdminTaskManager::init() {
    LOG(INFO) << "default subtask concurrency: " << FLAGS_subtask_concurrency;
    concurrentLimits.insert(FLAGS_subtask_concurrency);
    maxWorker_ = *concurrentLimits.begin();

    pool_ = std::make_unique<nebula::thread::GenericThreadPool>();
    CHECK(pool_->start(FLAGS_subtask_concurrency));
    for (auto i = 0U; i < FLAGS_subtask_concurrency; ++i) {
        pool_->addTask(&AdminTaskManager::pickSubTaskThread, this, i);
    }

    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
    bgThread_->addTask(&AdminTaskManager::pickTaskThread, this);

    shutdown_ = false;
    return true;
}

void AdminTaskManager::addAsyncTask(std::shared_ptr<AdminTask> task) {
    TaskHandle handle = std::make_pair(task->getJobId(), task->getTaskId());
    if (shutdown_) {
        LOG(ERROR) << folly::stringPrintf(
                      "trying to add task[%d, %d] after stop",
                      handle.first, handle.second);
        return;
    }

    LOG(INFO) << folly::stringPrintf("enqueue task(%d, %d), con req=%d",
                                     task->getJobId(), task->getTaskId(), task->getConcurrent());
    auto ctx = std::make_shared<TaskExecContext>(task);
    tasks_.insert(handle, ctx);
    taskHandleQueue_.add(handle);
}

nebula::kvstore::ResultCode
AdminTaskManager::cancelTask(int jobId) {
    LOG(INFO) << "AdminTaskManager::cancelTask() jobId=" << jobId;
    auto ret = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    auto it = tasks_.begin();
    while (it != tasks_.end()) {
        auto handle = it->first;
        if (handle.first == jobId) {
            it->second->task->cancel();
            LOG(INFO) << folly::stringPrintf("task[%d, %d] cancelled",
                                                jobId, handle.second);
            ret = kvstore::ResultCode::SUCCEEDED;
        }
        ++it;
    }
    return ret;
}

void AdminTaskManager::shutdown() {
    LOG(INFO) << "enter " << __PRETTY_FUNCTION__;
    shutdown_ = true;
    pool_->stop();
    pool_->wait();

    bgThread_->stop();
    bgThread_->wait();
    LOG(INFO) << "exit " << __PRETTY_FUNCTION__;
}

// [Todo] (liuyu)
// this will dequeue a task(t1) and hold it until
// satisify the concurrent requirement
// but it a hi priority task(t2) comes and meet the
// current concurrent requirement
// it can't be picked until t1 finished
void AdminTaskManager::pickTaskThread() {
    std::chrono::milliseconds interval{100};    // 100ms
    while (!shutdown_) {
        LOG(INFO) << "waiting for incoming task";
        folly::Optional<TaskHandle> optTaskHandle{folly::none};
        while (!optTaskHandle && !shutdown_) {
            optTaskHandle = taskHandleQueue_.try_take_for(interval);
        }

        if (shutdown_) {
            LOG(INFO) << "detect AdminTaskManager shutdown. exit";
            return;
        }

        auto taskHandle = *optTaskHandle;
        LOG(INFO) << folly::stringPrintf("dequeue task(%d, %d)",
                                         taskHandle.first, taskHandle.second);
        auto it = tasks_.find(taskHandle);
        if (it == tasks_.end()) {
            LOG(ERROR) << folly::stringPrintf(
                        "trying to exec non-exist task[%d, %d]",
                        taskHandle.first, taskHandle.second);
            continue;
        }

        std::shared_ptr<TaskExecContext> ctx = tasks_[taskHandle];
        auto task = ctx->task;
        while (!shutdown_) {
            if (task->cancelled()) {
                task->finish();
                tasks_.erase(taskHandle);
                break;
            }
            if (!satisifyConcurrency(task->getConcurrent())) {
                LOG(INFO) << folly::stringPrintf("waiting for task(%d, %d) satisify concurrency",
                                                 taskHandle.first, taskHandle.second);
                std::this_thread::sleep_for(interval);
                continue;
            }
            auto errOrSubTasks = task->genSubTasks();
            if (!nebula::ok(errOrSubTasks)) {
                task->finish(nebula::error(errOrSubTasks));
                tasks_.erase(taskHandle);
                break;
            }

            LOG(INFO) << folly::stringPrintf("extract sub tasks of task(%d, %d)",
                                             taskHandle.first, taskHandle.second);
            ctx->subTasks = nebula::value(errOrSubTasks);
            for (auto i = 0U; i < ctx->subTasks.size(); ++i) {
                auto subTaskHandle = std::make_tuple(taskHandle.first, taskHandle.second, i);
                subTaskHandleQueue_.add(subTaskHandle);
            }

            {
                std::lock_guard<std::mutex> lk(muConLimits_);
                concurrentLimits.insert(task->getConcurrent());
                maxWorker_ = *concurrentLimits.begin();
            }
            break;
        }
    }
}

void AdminTaskManager::pickSubTaskThread(int threadIndex) {
    int workerNum = threadIndex;
    bool threadActive = true;
    LOG(INFO) << "worker[" << workerNum << "] " << threadActive;
    std::chrono::milliseconds wakeup_interval{500};
    while (!shutdown_) {
        if (workerNum >= maxWorker_) {
            LOG(INFO) << folly::stringPrintf("in workerNum(%d) >= maxWorker_(%d)",
                                             workerNum, maxWorker_);
            if (threadActive) {
                int actWorker = --activeWorker_;
                threadActive = false;
                LOG(INFO) << folly::stringPrintf(
                            "workerNum(%d), maxWorker_(%d), threadActive(%d), actWorker(%d)",
                             workerNum, maxWorker_, threadActive, actWorker);
            }
            std::this_thread::sleep_for(wakeup_interval);
            continue;
        }

        auto optSubtaskHandle = subTaskHandleQueue_.try_take_for(wakeup_interval);
        if (optSubtaskHandle == folly::none) {
            if (threadActive) {
                threadActive = false;
                --activeWorker_;
            }
            std::this_thread::sleep_for(wakeup_interval);
            continue;
        }

        LOG(INFO) << "worker[" << workerNum << "] get sub task handle";
        if (!threadActive) {
            LOG(INFO) << "worker[" << workerNum << "] " << threadActive;
            ++activeWorker_;
            threadActive = true;
        }
        auto subtaskHandle = *optSubtaskHandle;
        auto taskHandle = std::make_pair(std::get<0>(subtaskHandle), std::get<1>(subtaskHandle));
        auto taskExecCtx = tasks_[taskHandle];
        auto subTaskIdx = std::get<2>(subtaskHandle);
        auto rc = taskExecCtx->subTasks[subTaskIdx].invoke();
        taskExecCtx->task->subFinish(rc);
        size_t finishedsubTasks = ++taskExecCtx->finishedsubTasks;
        // LOG(INFO) << "finishedsubTasks: " << finishedsubTasks;
        LOG(INFO) << folly::stringPrintf("%zu of %zu sub tasks finished (%d)",
                                         finishedsubTasks, taskExecCtx->subTasks.size(),
                                         static_cast<int>(rc));
        if (finishedsubTasks == taskExecCtx->subTasks.size()) {
            taskExecCtx->task->finish();
            tasks_.erase(taskHandle);
            {
                std::lock_guard<std::mutex> lk(muConLimits_);
                concurrentLimits.erase(taskExecCtx->task->getConcurrent());
                maxWorker_ = *concurrentLimits.begin();
            }
        }
    }
}

bool AdminTaskManager::satisifyConcurrency(int concurrentReqNum) {
    return maxWorker_ < concurrentReqNum || activeWorker_ < concurrentReqNum;
}

}  // namespace storage
}  // namespace nebula

