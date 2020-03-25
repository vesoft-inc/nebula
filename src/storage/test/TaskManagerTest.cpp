/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <chrono>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/admin/AdminTaskManager.h"

using namespace std::chrono_literals;   // NOLINT

namespace nebula {
namespace storage {

using ResultCode = nebula::kvstore::ResultCode;
using ErrOrSubTasks = ErrorOr<nebula::kvstore::ResultCode, std::vector<AdminSubTask>>;

auto suc = kvstore::ResultCode::SUCCEEDED;

struct HookableTask : public AdminTask {
    HookableTask() {
        fGenSubTasks = [&]() {
            return subTasks;
        };
    }
    ErrOrSubTasks genSubTasks() override {
        LOG(INFO) << "genSubTasks() subTasks.size()=" << subTasks.size();
        return fGenSubTasks();
    }

    void addSubTask(std::function<nebula::kvstore::ResultCode()> subTask) {
        subTasks.emplace_back(subTask);
    }

    void setJobId(int id) {
        jobId_ = id;
    }

    void setTaskId(int id) {
        taskId_ = id;
    }

    std::function<ErrOrSubTasks()> fGenSubTasks;
    std::vector<AdminSubTask> subTasks;
};

/*
 * summary:
 * 1. ctor
 * 2. happy_path:
 *      use 3 background thread to run 1, 2, 4, 8 sub tasks
 * 3. gen task failed directly
 *      check task manger will return the right error code
 * 4. some task return error code.
 *      check task manger will return the right error code
 * 5. cancel task in task queue
 *      5.1 cancel_a_running_task_with_only_1_sub_task
 *      5.2 cancel_1_task_in_a_2_tasks_queue
 * 6. cancel some sub task
 *      6.1 cancel_a_task_before_all_sub_task_running
 *      6.2 cancel_a_task_while_some_sub_task_running
 * */

// 0. basic data structure
TEST(TaskManagerTest, basic_data_structure) {
    using TKey = std::pair<int, int>;
    using TVal = std::shared_ptr<AdminTaskManager::TaskExecContext>;
    using TaskContainer = folly::ConcurrentHashMap<TKey, TVal>;
    // CRUD
    // create
    std::shared_ptr<AdminTask> t1(new HookableTask());
    TaskContainer tasks;
    TKey k0 = std::make_pair(1, 1);
    TVal ctx0 = std::make_shared<AdminTaskManager::TaskExecContext>(t1);
    EXPECT_TRUE(tasks.empty());
    EXPECT_EQ(tasks.find(k0), tasks.cend());
    auto r = tasks.insert(k0, ctx0);
    EXPECT_TRUE(r.second);

    // read
    auto cit = tasks.find(k0);

    // update
    auto newTask = tasks[k0];
    newTask->task = t1;

    // delete
    auto nRemove = tasks.erase(k0);
    EXPECT_EQ(1, nRemove);
    EXPECT_TRUE(tasks.empty());
}

std::atomic<int> conLimit;

void mockPickSubTaskThread(int thread_idx) {
    LOG(INFO) << "thread_idx: " << thread_idx;
    while (conLimit > 0) {
        if (thread_idx < conLimit) {
            LOG(INFO) << folly::stringPrintf("exceed max conLimit waiting");
            std::this_thread::sleep_for(100ms);
        }
    }
}

TEST(TaskManagerTest, concurrency_control) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    int maxThread = 5;

    using Pool = nebula::thread::GenericThreadPool;
    std::unique_ptr<Pool> pool_ = std::make_unique<Pool>();

    pool_->start(maxThread);

    int thread_idx = 0;
    // std::atomic<int> conLimit;
    conLimit = maxThread;

    for (int i = 0; i < maxThread; ++i) {
        pool_->addTask(mockPickSubTaskThread, thread_idx);
    }

    LOG(INFO) << "before sleep_for(2s)";
    std::this_thread::sleep_for(1s);
    LOG(INFO) << "after sleep_for(2s)";
    conLimit = -1;
    pool_->stop();
    pool_->wait();
    taskMgr->shutdown();
}

// 1. ctor
TEST(TaskManagerTest, ctor) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    taskMgr->shutdown();
}

TEST(TaskManagerTest, happy_path_task1_sub1) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();

    size_t numSubTask = 1;
    std::shared_ptr<AdminTask> task(new HookableTask());
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
    folly::Promise<ResultCode> pro;
    folly::Future<ResultCode> fut = pro.getFuture();

    auto taskCallback = [&](ResultCode ret) {
        LOG(INFO) << "taskCallback";
        pro.setValue(ret);
    };
    mockTask->setCallback(taskCallback);
    size_t subTaskCalled = 0;

    auto subTask = [&subTaskCalled]() {
        ++subTaskCalled;
        LOG(INFO) << "subTask invoke()";
        return kvstore::ResultCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
        mockTask->addSubTask(subTask);
    }
    taskMgr->addAsyncTask(task);
    fut.wait();

    EXPECT_EQ(numSubTask, subTaskCalled);

    taskMgr->shutdown();
}

TEST(TaskManagerTest, happy_path) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    taskMgr->setSubTaskLimit(3);

    {
        size_t numSubTask = 1;
        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();

        auto taskCallback = [&](ResultCode ret) {
            pro.setValue(ret);
        };
        mockTask->setCallback(taskCallback);
        size_t subTaskCalled = 0;

        auto subTask = [&subTaskCalled]() {
            ++subTaskCalled;
            return kvstore::ResultCode::SUCCEEDED;
        };
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask(subTask);
        }
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
    }

    {   // use 3 background to run 2 task
        size_t numSubTask = 2;
        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        int jobId = 2;
        mockTask->setJobId(jobId);
        mockTask->setTaskId(jobId);
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();

        auto taskCallback = [&](ResultCode ret) {
            pro.setValue(ret);
        };
        mockTask->setCallback(taskCallback);
        size_t subTaskCalled = 0;

        auto subTask = [&subTaskCalled]() {
            ++subTaskCalled;
            return kvstore::ResultCode::SUCCEEDED;
        };
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask(subTask);
        }
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
    }

    {   // use 3 background to run 3 task
        size_t numSubTask = 3;
        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();
        int jobId = 3;
        mockTask->setJobId(jobId);
        mockTask->setTaskId(jobId);

        auto taskCallback = [&](ResultCode ret) {
            pro.setValue(ret);
        };
        mockTask->setCallback(taskCallback);
        size_t subTaskCalled = 0;

        auto subTask = [&subTaskCalled]() {
            ++subTaskCalled;
            return kvstore::ResultCode::SUCCEEDED;
        };
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask(subTask);
        }
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
    }

    {   // use 3 background to run 4 task
        size_t numSubTask = 4;
        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        int jobId = 4;
        mockTask->setJobId(jobId);
        mockTask->setTaskId(jobId);
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();

        auto taskCallback = [&](ResultCode ret) {
            pro.setValue(ret);
        };
        mockTask->setCallback(taskCallback);
        size_t subTaskCalled = 0;
        auto subTask = [&subTaskCalled]() {
            ++subTaskCalled;
            return kvstore::ResultCode::SUCCEEDED;
        };
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask(subTask);
        }
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
    }

    {   // use 3 background to run 8 task
        size_t numSubTask = 8;
        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        int jobId = 5;
        mockTask->setJobId(jobId);
        mockTask->setTaskId(jobId);
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();

        auto taskCallback = [&](ResultCode ret) {
            pro.setValue(ret);
        };
        mockTask->setCallback(taskCallback);
        size_t subTaskCalled = 0;
        auto subTask = [&subTaskCalled]() {
            ++subTaskCalled;
            return kvstore::ResultCode::SUCCEEDED;
        };
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask(subTask);
        }
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
    }

    taskMgr->shutdown();
}

TEST(TaskManagerTest, gen_sub_task_failed) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();

    {
        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        mockTask->fGenSubTasks = [&]() {
            return kvstore::ResultCode::ERR_UNSUPPORTED;
        };

        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();

        auto cb = [&](ResultCode ret) {
            pro.setValue(ret);
        };

        mockTask->setCallback(cb);
        taskMgr->addAsyncTask(task);

        fut.wait();

        EXPECT_EQ(fut.value(), kvstore::ResultCode::ERR_UNSUPPORTED);
    }

    taskMgr->shutdown();
}

TEST(TaskManagerTest, some_subtask_failed) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    {
        size_t numSubTask = 1;
        size_t subTaskCalled = 0;
        auto errorCode = kvstore::ResultCode::ERR_PART_NOT_FOUND;

        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = static_cast<HookableTask*>(task.get());
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask([&]() {
                ++subTaskCalled;
                return i == numSubTask / 2 ? suc : errorCode;
            });
        }
        mockTask->setCallback([&](ResultCode ret) {
            pro.setValue(ret);
        });
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
        EXPECT_EQ(fut.value(), errorCode);
    }

    {
        size_t numSubTask = 2;
        size_t subTaskCalled = 0;
        auto errorCode = kvstore::ResultCode::ERR_LEADER_CHANGED;

        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = static_cast<HookableTask*>(task.get());
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask([&]() {
                ++subTaskCalled;
                return i == numSubTask / 2 ? suc : errorCode;
            });
        }
        mockTask->setCallback([&](ResultCode ret) {
            pro.setValue(ret);
        });
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
        EXPECT_EQ(fut.value(), errorCode);
    }

    {
        size_t numSubTask = 4;
        size_t subTaskCalled = 0;
        auto errorCode = kvstore::ResultCode::ERR_TAG_NOT_FOUND;

        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = static_cast<HookableTask*>(task.get());
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask([&]() {
                ++subTaskCalled;
                return i == numSubTask / 2 ? suc : errorCode;
            });
        }
        mockTask->setCallback([&](ResultCode ret) {
            pro.setValue(ret);
        });
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
        EXPECT_EQ(fut.value(), errorCode);
    }

    {
        size_t numSubTask = 8;
        size_t subTaskCalled = 0;
        auto errorCode = kvstore::ResultCode::ERR_UNKNOWN;

        std::shared_ptr<AdminTask> task(new HookableTask());
        HookableTask* mockTask = static_cast<HookableTask*>(task.get());
        folly::Promise<ResultCode> pro;
        folly::Future<ResultCode> fut = pro.getFuture();
        for (size_t i = 0; i < numSubTask; ++i) {
            mockTask->addSubTask([&]() {
                ++subTaskCalled;
                return i == numSubTask / 2 ? suc : errorCode;
            });
        }
        mockTask->setCallback([&](ResultCode ret) {
            pro.setValue(ret);
        });
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_EQ(numSubTask, subTaskCalled);
        EXPECT_EQ(fut.value(), errorCode);
    }

    taskMgr->shutdown();
}

TEST(TaskManagerTest, cancel_a_running_task_with_only_1_sub_task) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    size_t numTask = 1;

    std::shared_ptr<AdminTask> task(new HookableTask());
    HookableTask* mockTask = static_cast<HookableTask*>(task.get());

    folly::Promise<int> pTaskRun;
    folly::Future<int> fTaskRun = pTaskRun.getFuture();

    folly::Promise<ResultCode> taskFinishedPro;
    folly::Future<ResultCode> taskFinished = taskFinishedPro.getFuture();

    folly::Promise<int> taskCancelledPro;
    folly::Future<int> taskCancelledFut = taskCancelledPro.getFuture();

    mockTask->addSubTask([&]() {
        LOG(INFO) << "sub task running, waiting for cancel";
        pTaskRun.setValue(0);
        taskCancelledFut.wait();
        LOG(INFO) << "cancel called";
        return suc;
    });

    mockTask->setCallback([&](ResultCode ret) {
        LOG(INFO) << "task callback()";
        ++numTask;
        taskFinishedPro.setValue(ret);
    });

    mockTask->setJobId(1);
    taskMgr->addAsyncTask(task);
    fTaskRun.wait();
    taskMgr->cancelTask(1);
    taskCancelledPro.setValue(0);
    taskFinished.wait();

    taskMgr->shutdown();
    EXPECT_EQ(taskCancelledFut.value(), suc);
}

TEST(TaskManagerTest, cancel_1_task_in_a_2_tasks_queue) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();

    auto err = kvstore::ResultCode::ERR_USER_CANCELLED;
    // add 2 task into queue
    // cancel task1
    // check 1: task0 suc
    // check 2: task1 err
    folly::Promise<ResultCode> pTask1;
    auto fTask1 = pTask1.getFuture();

    folly::Promise<int> pRunTask1;
    auto fRunTask1 = pRunTask1.getFuture();

    folly::Promise<ResultCode> pTask2;
    folly::Future<ResultCode> fTask2 = pTask2.getFuture();

    folly::Promise<int> pCancelTask2;
    folly::Future<int> fCancelTask2 = pCancelTask2.getFuture();

    std::shared_ptr<AdminTask> vtask1(new HookableTask());
    HookableTask* task1 = static_cast<HookableTask*>(vtask1.get());
    task1->setJobId(1);

    task1->addSubTask([&]() {
        pRunTask1.setValue(0);
        fCancelTask2.wait();
        std::this_thread::sleep_for(100ms);
        return suc;
    });
    task1->setCallback([&](ResultCode ret) {
        LOG(INFO) << "finish task1()";
        pTask1.setValue(ret);
    });

    std::shared_ptr<AdminTask> vtask2(new HookableTask());
    HookableTask* task2 = static_cast<HookableTask*>(vtask2.get());
    task2->setJobId(2);

    task2->addSubTask([&]() {
        return suc;
    });
    task2->setCallback([&](ResultCode ret) {
        LOG(INFO) << "finish task2()";
        pTask2.setValue(ret);
    });
    taskMgr->addAsyncTask(vtask1);
    taskMgr->addAsyncTask(vtask2);

    fRunTask1.wait();
    taskMgr->cancelTask(2);
    pCancelTask2.setValue(0);

    fTask1.wait();
    fTask2.wait();

    EXPECT_EQ(fTask1.value(), suc);
    EXPECT_EQ(fTask2.value(), err);

    taskMgr->shutdown();
}

TEST(TaskManagerTest, cancel_a_task_before_all_sub_task_running) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    auto err = kvstore::ResultCode::ERR_USER_CANCELLED;

    int jobId = 1;
    folly::Promise<ResultCode> pFiniTask0;
    auto fFiniTask0 = pFiniTask0.getFuture();

    folly::Promise<int> pRunTask;
    auto fRunTask = pRunTask.getFuture();

    folly::Promise<int> pCancelTask;
    auto fCancelTask = pCancelTask.getFuture();

    std::shared_ptr<AdminTask> vtask0(new HookableTask());
    HookableTask* task0 = static_cast<HookableTask*>(vtask0.get());
    task0->setJobId(jobId);

    task0->fGenSubTasks = [&]() {
        pRunTask.setValue(0);
        fCancelTask.wait();
        return task0->subTasks;
    };

    task0->addSubTask([&]() {
        LOG(INFO) << "run subTask()";
        return suc;
    });

    task0->setCallback([&](ResultCode ret) {
        pFiniTask0.setValue(ret);
    });

    taskMgr->addAsyncTask(vtask0);

    LOG(INFO) << "before taskMgr->cancelTask(1);";
    fRunTask.wait();
    taskMgr->cancelTask(jobId);
    LOG(INFO) << "after taskMgr->cancelTask(1);";
    pCancelTask.setValue(0);

    fFiniTask0.wait();

    EXPECT_EQ(fFiniTask0.value(), err);

    taskMgr->shutdown();
}

TEST(TaskManagerTest, cancel_a_task_while_some_sub_task_running) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    taskMgr->setSubTaskLimit(1);
    auto err = kvstore::ResultCode::ERR_USER_CANCELLED;

    folly::Promise<ResultCode> task1_p;
    folly::Future<ResultCode> task1_f = task1_p.getFuture();

    folly::Promise<int> cancle_p;
    folly::Future<int> cancel = cancle_p.getFuture();

    folly::Promise<int> subtask_run_p;
    folly::Future<int> subtask_run_f = subtask_run_p.getFuture();

    std::shared_ptr<AdminTask> vtask0(new HookableTask());
    HookableTask* task1 = static_cast<HookableTask*>(vtask0.get());
    task1->setJobId(1);

    task1->addSubTask([&]() {
        LOG(INFO) << "wait for cancel()";
        subtask_run_p.setValue(0);
        cancel.wait();
        LOG(INFO) << "run subTask(1)";
        return suc;
    });

    task1->addSubTask([&]() {
        LOG(INFO) << "run subTask(2)";
        return suc;
    });

    task1->setCallback([&](ResultCode ret) {
        task1_p.setValue(ret);
    });

    taskMgr->addAsyncTask(vtask0);

    subtask_run_f.wait();
    LOG(INFO) << "before taskMgr->cancelTask(1);";
    taskMgr->cancelTask(1);
    LOG(INFO) << "after taskMgr->cancelTask(1);";
    cancle_p.setValue(0);

    task1_f.wait();

    EXPECT_EQ(task1_f.value(), err);

    taskMgr->shutdown();
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
