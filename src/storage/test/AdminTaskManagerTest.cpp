/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "storage/admin/AdminTaskManager.h"

using namespace std::chrono_literals;   // NOLINT

namespace nebula {
namespace storage {
/*
 * summary:
 * 0. validation for some basic data structure and component
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

using ErrOrSubTasks = ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>;

auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
int gJobId = 0;

struct HookableTask : public AdminTask {
    HookableTask() {
        fGenSubTasks = [&]() {
            return subTasks;
        };
    }
    ErrOrSubTasks genSubTasks() override {
        LOG(INFO) << "HookableTask::genSubTasks() subTasks.size()=" << subTasks.size();
        return fGenSubTasks();
    }

    void addSubTask(std::function<nebula::cpp2::ErrorCode()> subTask) {
        subTasks.emplace_back(subTask);
    }

    void setJobId(int id) {
        ctx_.jobId_ = id;
    }

    void setTaskId(int id) {
        ctx_.taskId_ = id;
    }

    std::function<ErrOrSubTasks()> fGenSubTasks;
    std::vector<AdminSubTask> subTasks;
};

// *** 0. basic component
TEST(TaskManagerTest, extract_subtasks_to_context) {
    size_t numSubTask = 5;
    auto vtask = std::make_shared<HookableTask>();
    HookableTask* task = static_cast<HookableTask*>(vtask.get());
    int subTaskCalled = 0;
    for (size_t i = 0; i < numSubTask; ++i) {
        task->addSubTask([&subTaskCalled]() {
            ++subTaskCalled;
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        });
    }
    for (auto& subtask : task->subTasks) {
        vtask->subtasks_.add(subtask);
    }

    std::chrono::milliseconds ms10{10};
    while (auto it = vtask->subtasks_.try_take_for(ms10)) {
        auto& subTask = *it;
        subTask.invoke();
    }

    EXPECT_EQ(subTaskCalled, numSubTask);
}

using Func = std::function<void()>;
static Func burnMs(uint64_t ms) {
  return [ms]() { std::this_thread::sleep_for(
                  std::chrono::milliseconds(ms)); };
}

TEST(TaskManagerTest, component_IOThreadPool) {
    using ThreadPool = folly::IOThreadPoolExecutor;
    size_t numThreads = 5;
    {
        std::atomic<int> completed(0);
        auto f = [&]() {
            burnMs(1)();
            ++completed;
        };
        auto pool = std::make_unique<ThreadPool>(numThreads);
        pool->add(f);
        pool->join();
        EXPECT_EQ(completed, 1);
    }

    {
        int totalTask = 100;
        std::mutex                  mu;
        std::set<std::thread::id>   threadIds;
        auto fo = [&]() {
            std::lock_guard<std::mutex> lk(mu);
            threadIds.insert(std::this_thread::get_id());
        };
        auto pool = std::make_unique<ThreadPool>(numThreads);
        for (int i = 0; i < totalTask; ++i) {
            pool->add(fo);
        }
        pool->join();

        LOG(INFO) << "threadIds.size()=" << threadIds.size();
        EXPECT_LE(threadIds.size(), numThreads);
    }

    {
        int totalTask = 100;
        using Para = folly::Optional<std::thread::id>;
        std::mutex mu;
        int completed = 0;
        auto pool = std::make_unique<ThreadPool>(numThreads);
        std::function<void(Para)> f = [&](Para optTid) {
            auto tid = std::this_thread::get_id();
            if (optTid) {
                EXPECT_EQ(tid, *optTid);
            }
            burnMs(10);
            {
                std::lock_guard<std::mutex> lk(mu);
                if (completed < totalTask) {
                    ++completed;
                } else {
                    return;
                }
            }
            pool->add(std::bind(f, tid));
        };

        for (int i = 0; i < totalTask; ++i) {
            pool->add(std::bind(f, folly::none));
        }
        pool->join();

        EXPECT_EQ(completed, totalTask);
    }
}

TEST(TaskManagerTest, data_structure_ConcurrentHashMap) {
    using TKey = std::pair<int, int>;
    using TVal = std::shared_ptr<AdminTask>;
    using TaskContainer = folly::ConcurrentHashMap<TKey, TVal>;
    // CRUD
    // create
    std::shared_ptr<AdminTask> t1 = std::make_shared<HookableTask>();
    TaskContainer tasks;
    TKey k0 = std::make_pair(1, 1);
    EXPECT_TRUE(tasks.empty());
    EXPECT_EQ(tasks.find(k0), tasks.cend());
    auto r = tasks.insert(k0, t1);
    EXPECT_TRUE(r.second);

    // read
    auto cit = tasks.find(k0);

    // update
    auto newTask = tasks[k0];
    newTask  = t1;

    // delete
    auto nRemove = tasks.erase(k0);
    EXPECT_EQ(1, nRemove);
    EXPECT_TRUE(tasks.empty());
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
    std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
    int jobId = ++gJobId;

    mockTask->setJobId(jobId);
    mockTask->setTaskId(jobId);
    folly::Promise<nebula::cpp2::ErrorCode> pTaskFini;

    auto fTaskFini = pTaskFini.getFuture();

    auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
        LOG(INFO) << "taskCallback";
        pTaskFini.setValue(ret);
    };
    mockTask->setCallback(taskCallback);
    size_t subTaskCalled = 0;

    auto subTask = [&subTaskCalled]() {
        ++subTaskCalled;
        LOG(INFO) << "subTask invoke()";
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
        mockTask->addSubTask(subTask);
    }
    taskMgr->addAsyncTask(task);
    LOG(INFO) << "fTaskFini.wait(0)";
    fTaskFini.wait();
    LOG(INFO) << "fTaskFini.wait(1)";

    EXPECT_EQ(numSubTask, subTaskCalled);

    taskMgr->shutdown();
}

TEST(TaskManagerTest, run_a_medium_task_before_a_huge_task) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();

    {   // 256 sub tasks
        size_t numSubTask = 256;
        std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        int jobId = ++gJobId;

        mockTask->setJobId(jobId);
        mockTask->setTaskId(jobId);
        folly::Promise<nebula::cpp2::ErrorCode> pro;
        folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

        auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
            pro.setValue(ret);
        };
        mockTask->setCallback(taskCallback);
        std::atomic<int> subTaskCalled{0};
        auto subTask = [&subTaskCalled]() {
            ++subTaskCalled;
            return nebula::cpp2::ErrorCode::SUCCEEDED;
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

TEST(TaskManagerTest, happy_path) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
        {
            size_t numSubTask = 1;
            std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
            HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
            int jobId = ++gJobId;
            mockTask->setJobId(jobId);
            mockTask->setTaskId(jobId);
            folly::Promise<nebula::cpp2::ErrorCode> pro;
            folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

            auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
                pro.setValue(ret);
            };
            mockTask->setCallback(taskCallback);
            size_t subTaskCalled = 0;

            auto subTask = [&subTaskCalled]() {
                ++subTaskCalled;
                return nebula::cpp2::ErrorCode::SUCCEEDED;
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
            std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
            HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
            int jobId = ++gJobId;
            mockTask->setJobId(jobId);
            mockTask->setTaskId(jobId);
            folly::Promise<nebula::cpp2::ErrorCode> pro;
            folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

            auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
                pro.setValue(ret);
            };
            mockTask->setCallback(taskCallback);
            std::atomic<int> subTaskCalled{0};

            auto subTask = [&subTaskCalled]() {
                ++subTaskCalled;
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            };
            for (size_t i = 0; i < numSubTask; ++i) {
                mockTask->addSubTask(subTask);
            }
            taskMgr->addAsyncTask(task);
            fut.wait();

            ASSERT_EQ(numSubTask, subTaskCalled);
        }

        {   // use 3 background to run 3 task
            size_t numSubTask = 3;
            std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
            HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
            folly::Promise<nebula::cpp2::ErrorCode> pro;
            folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();
            int jobId = ++gJobId;
            mockTask->setJobId(jobId);
            mockTask->setTaskId(jobId);

            auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
                pro.setValue(ret);
            };
            mockTask->setCallback(taskCallback);
            std::atomic<int> subTaskCalled{0};

            auto subTask = [&subTaskCalled]() {
                ++subTaskCalled;
                return nebula::cpp2::ErrorCode::SUCCEEDED;
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
            std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
            HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
            int jobId = ++gJobId;
            mockTask->setJobId(jobId);
            mockTask->setTaskId(jobId);
            folly::Promise<nebula::cpp2::ErrorCode> pro;
            folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

            auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
                pro.setValue(ret);
            };
            mockTask->setCallback(taskCallback);
            std::atomic<int> subTaskCalled{0};
            auto subTask = [&subTaskCalled]() {
                ++subTaskCalled;
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            };
            for (size_t i = 0; i < numSubTask; ++i) {
                mockTask->addSubTask(subTask);
            }
            taskMgr->addAsyncTask(task);
            fut.wait();

            EXPECT_EQ(numSubTask, subTaskCalled);
        }

        {   // 8 task
            size_t numSubTask = 8;
            std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
            HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
            int jobId = ++gJobId;
            mockTask->setJobId(jobId);
            mockTask->setTaskId(jobId);
            folly::Promise<nebula::cpp2::ErrorCode> pro;
            folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

            auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
                pro.setValue(ret);
            };
            mockTask->setCallback(taskCallback);
            std::atomic<int> subTaskCalled{0};
            auto subTask = [&subTaskCalled]() {
                ++subTaskCalled;
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            };
            for (size_t i = 0; i < numSubTask; ++i) {
                mockTask->addSubTask(subTask);
            }
            taskMgr->addAsyncTask(task);
            fut.wait();

            EXPECT_EQ(numSubTask, subTaskCalled);
        }

        {   // 16 task
            size_t numSubTask = 16;
            std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
            HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
            int jobId = ++gJobId;
            mockTask->setJobId(jobId);
            mockTask->setTaskId(jobId);
            folly::Promise<nebula::cpp2::ErrorCode> pro;
            folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

            auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
                pro.setValue(ret);
            };
            mockTask->setCallback(taskCallback);
            std::atomic<int> subTaskCalled{0};
            auto subTask = [&subTaskCalled]() {
                ++subTaskCalled;
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            };
            for (size_t i = 0; i < numSubTask; ++i) {
                mockTask->addSubTask(subTask);
            }
            taskMgr->addAsyncTask(task);
            fut.wait();

            EXPECT_EQ(numSubTask, subTaskCalled);
        }

        {   // 256 sub tasks
            size_t numSubTask = 256;
            std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
            HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
            int jobId = ++gJobId;

            mockTask->setJobId(jobId);
            mockTask->setTaskId(jobId);
            folly::Promise<nebula::cpp2::ErrorCode> pro;
            folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

            auto taskCallback = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
                pro.setValue(ret);
            };
            mockTask->setCallback(taskCallback);
            std::atomic<int> subTaskCalled{0};
            auto subTask = [&subTaskCalled]() {
                ++subTaskCalled;
                return nebula::cpp2::ErrorCode::SUCCEEDED;
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
        std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
        HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
        mockTask->fGenSubTasks = [&]() {
            return nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA;
        };

        folly::Promise<nebula::cpp2::ErrorCode> pro;
        folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();

        auto cb = [&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
            pro.setValue(ret);
        };

        mockTask->setCallback(cb);
        taskMgr->addAsyncTask(task);

        fut.wait();

        EXPECT_EQ(fut.value(), nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA);
    }

    taskMgr->shutdown();
}

TEST(TaskManagerTest, some_subtask_failed) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    std::vector<nebula::cpp2::ErrorCode> errorCode {
        nebula::cpp2::ErrorCode::SUCCEEDED,
        nebula::cpp2::ErrorCode::E_PART_NOT_FOUND,
        nebula::cpp2::ErrorCode::E_LEADER_CHANGED,
        nebula::cpp2::ErrorCode::E_USER_CANCEL,
        nebula::cpp2::ErrorCode::E_UNKNOWN
    };

    for (auto t = ++gJobId; t < 5; ++t) {
        int jobId = t;
        size_t totalSubTask = std::pow(4, t);
        std::atomic<int> subTaskCalled{0};
        auto errCode = errorCode[t];

        // std::shared_ptr<AdminTask> task(new HookableTask());
        std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
        HookableTask* mockTask = static_cast<HookableTask*>(task.get());
        mockTask->setJobId(jobId);
        mockTask->setTaskId(jobId);

        folly::Promise<nebula::cpp2::ErrorCode> pro;
        folly::Future<nebula::cpp2::ErrorCode> fut = pro.getFuture();
        for (size_t i = 0; i < totalSubTask; ++i) {
            mockTask->addSubTask([&]() {
                ++subTaskCalled;
                return i == totalSubTask / 2 ? suc : errCode;
            });
        }
        mockTask->setCallback([&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
            pro.setValue(ret);
        });
        taskMgr->addAsyncTask(task);
        fut.wait();

        EXPECT_LE(subTaskCalled, totalSubTask);
        EXPECT_EQ(fut.value(), errCode);
    }

    taskMgr->shutdown();
}

TEST(TaskManagerTest, cancel_a_running_task_with_only_1_sub_task) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    int jobId = ++gJobId;

    std::shared_ptr<AdminTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = static_cast<HookableTask*>(task.get());
    mockTask->setJobId(jobId);

    folly::Promise<int> pTaskRun;
    auto fTaskRun = pTaskRun.getFuture();

    folly::Promise<nebula::cpp2::ErrorCode> pFinish;
    folly::Future<nebula::cpp2::ErrorCode> fFinish = pFinish.getFuture();

    folly::Promise<int> pCancel;
    folly::Future<int> fCancel = pCancel.getFuture();

    mockTask->addSubTask([&]() {
        LOG(INFO) << "sub task running, waiting for cancel";
        pTaskRun.setValue(0);
        fCancel.wait();
        LOG(INFO) << "cancel called";
        return suc;
    });

    mockTask->setCallback([&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
        LOG(INFO) << "task finish()";
        pFinish.setValue(ret);
    });

    taskMgr->addAsyncTask(task);

    fTaskRun.wait();
    taskMgr->cancelTask(jobId);
    pCancel.setValue(0);
    fFinish.wait();

    taskMgr->shutdown();
    auto err = nebula::cpp2::ErrorCode::E_USER_CANCEL;
    EXPECT_EQ(fFinish.value(), err);
}

TEST(TaskManagerTest, cancel_1_task_in_a_2_tasks_queue) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    int jobId1 = ++gJobId;
    int jobId2 = ++gJobId;

    auto err = nebula::cpp2::ErrorCode::E_USER_CANCEL;

    folly::Promise<nebula::cpp2::ErrorCode> pTask1;
    auto fTask1 = pTask1.getFuture();

    folly::Promise<int> pRunTask1;
    auto fRunTask1 = pRunTask1.getFuture();

    folly::Promise<nebula::cpp2::ErrorCode> pTask2;
    folly::Future<nebula::cpp2::ErrorCode> fTask2 = pTask2.getFuture();

    folly::Promise<int> pCancelTask2;
    folly::Future<int> fCancelTask2 = pCancelTask2.getFuture();

    std::shared_ptr<AdminTask> vtask1 = std::make_shared<HookableTask>();
    HookableTask* task1 = static_cast<HookableTask*>(vtask1.get());
    task1->setJobId(jobId1);

    task1->fGenSubTasks = [&]() {
        pRunTask1.setValue(0);
        fCancelTask2.wait();
        return task1->subTasks;
    };

    task1->addSubTask([&]() {
        return suc;
    });
    task1->setCallback([&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
        LOG(INFO) << "finish task1()";
        pTask1.setValue(ret);
    });

    std::shared_ptr<AdminTask> vtask2 = std::make_shared<HookableTask>();
    HookableTask* task2 = static_cast<HookableTask*>(vtask2.get());
    task2->setJobId(jobId2);

    task2->addSubTask([&]() {
        return suc;
    });
    task2->setCallback([&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
        LOG(INFO) << "finish task2()";
        pTask2.setValue(ret);
    });
    taskMgr->addAsyncTask(vtask1);
    taskMgr->addAsyncTask(vtask2);

    fRunTask1.wait();
    taskMgr->cancelTask(jobId2);
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
    auto err = nebula::cpp2::ErrorCode::E_USER_CANCEL;

    int jobId = ++gJobId;
    folly::Promise<nebula::cpp2::ErrorCode> pFiniTask0;
    auto fFiniTask0 = pFiniTask0.getFuture();

    folly::Promise<int> pRunTask;
    auto fRunTask = pRunTask.getFuture();

    folly::Promise<int> pCancelTask;
    auto fCancelTask = pCancelTask.getFuture();

    std::shared_ptr<AdminTask> vtask0 = std::make_shared<HookableTask>();
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

    task0->setCallback([&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
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

TEST(TaskManagerTest, task_run_after_a_gen_sub_task_failed) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();

    std::shared_ptr<AdminTask> vtask1 = std::make_shared<HookableTask>();
    std::shared_ptr<AdminTask> vtask2 = std::make_shared<HookableTask>();

    HookableTask* task1 = dynamic_cast<HookableTask*>(vtask1.get());
    HookableTask* task2 = dynamic_cast<HookableTask*>(vtask2.get());

    task1->setJobId(++gJobId);
    task2->setJobId(++gJobId);

    folly::Promise<int> p0;
    folly::Promise<int> p1;
    folly::Promise<int> p2;

    auto f0 = p0.getFuture();
    auto f1 = p1.getFuture();
    auto f2 = p2.getFuture();

    task1->fGenSubTasks = [&]() mutable {
        LOG(INFO) << "before f1.wait()";
        f1.wait();
        LOG(INFO) << "after f1.wait()";
        return nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA;
    };
    task1->setCallback([&](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&){
        LOG(INFO) << "before p2.setValue()";
        p2.setValue(1);
        LOG(INFO) << "after p2.setValue()";
    });
    task2->addSubTask([&]() mutable {
        LOG(INFO) << "before f2.wait()";
        f2.wait();
        LOG(INFO) << "after f2.wait()";
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    });
    task2->setCallback([&](nebula::cpp2::ErrorCode, nebula::meta::cpp2::StatisItem&){
        LOG(INFO) << "before p0.setValue()";
        p0.setValue(1);
        LOG(INFO) << "after p0.setValue()";
    });

    taskMgr->addAsyncTask(vtask1);
    taskMgr->addAsyncTask(vtask2);
    p1.setValue(1);

    LOG(INFO) << "before p0.setValue()";
    f0.wait();
    LOG(INFO) << "after p0.setValue()";

    taskMgr->shutdown();
}

TEST(TaskManagerTest, cancel_a_task_while_some_sub_task_running) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    auto usr_cancel = nebula::cpp2::ErrorCode::E_USER_CANCEL;
    int jobId = ++gJobId;

    folly::Promise<nebula::cpp2::ErrorCode> task1_p;
    folly::Future<nebula::cpp2::ErrorCode> task1_f = task1_p.getFuture();

    folly::Promise<int> cancle_p;
    folly::Future<int> cancel = cancle_p.getFuture();

    folly::Promise<int> subtask_run_p;
    folly::Future<int> subtask_run_f = subtask_run_p.getFuture();

    std::shared_ptr<AdminTask> vtask0 = std::make_shared<HookableTask>();
    HookableTask* task1 = static_cast<HookableTask*>(vtask0.get());
    task1->setJobId(jobId);

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

    task1->setCallback([&](nebula::cpp2::ErrorCode ret, nebula::meta::cpp2::StatisItem&) {
        task1_p.setValue(ret);
    });

    taskMgr->addAsyncTask(vtask0);

    subtask_run_f.wait();
    LOG(INFO) << "before taskMgr->cancelTask(1);";
    taskMgr->cancelTask(jobId);
    LOG(INFO) << "after taskMgr->cancelTask(1);";
    cancle_p.setValue(0);

    task1_f.wait();

    EXPECT_EQ(task1_f.value(), usr_cancel);

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
