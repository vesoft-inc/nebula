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
 *      6.1 cancel_a_running_task_with_2_sub_task
 * */

// TEST(TaskManagerTest, ctor) {
//     auto taskMgr = AdminTaskManager::instance();
//     taskMgr->init();
//     taskMgr->shutdown();
// }

// TEST(TaskManagerTest, happy_path) {
//     auto taskMgr = AdminTaskManager::instance();
//     taskMgr->init();
//     taskMgr->setSubTaskLimit(3);

//     {
//         size_t numSubTask = 1;
//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();

//         auto taskCallback = [&](ResultCode ret) {
//             pro.setValue(ret);
//         };
//         mockTask->setCallback(taskCallback);
//         size_t subTaskCalled = 0;

//         auto subTask = [&subTaskCalled]() {
//             ++subTaskCalled;
//             return kvstore::ResultCode::SUCCEEDED;
//         };
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask(subTask);
//         }
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//     }

//     {   // use 3 background to run 2 task
//         size_t numSubTask = 2;
//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();

//         auto taskCallback = [&](ResultCode ret) {
//             pro.setValue(ret);
//         };
//         mockTask->setCallback(taskCallback);
//         size_t subTaskCalled = 0;

//         auto subTask = [&subTaskCalled]() {
//             ++subTaskCalled;
//             return kvstore::ResultCode::SUCCEEDED;
//         };
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask(subTask);
//         }
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//     }

//     {   // use 3 background to run 3 task
//         size_t numSubTask = 3;
//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();

//         auto taskCallback = [&](ResultCode ret) {
//             pro.setValue(ret);
//         };
//         mockTask->setCallback(taskCallback);
//         size_t subTaskCalled = 0;

//         auto subTask = [&subTaskCalled]() {
//             ++subTaskCalled;
//             return kvstore::ResultCode::SUCCEEDED;
//         };
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask(subTask);
//         }
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//     }

//     {   // use 3 background to run 4 task
//         size_t numSubTask = 4;
//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();

//         auto taskCallback = [&](ResultCode ret) {
//             pro.setValue(ret);
//         };
//         mockTask->setCallback(taskCallback);
//         size_t subTaskCalled = 0;
//         auto subTask = [&subTaskCalled]() {
//             ++subTaskCalled;
//             return kvstore::ResultCode::SUCCEEDED;
//         };
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask(subTask);
//         }
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//     }

//     {   // use 3 background to run 8 task
//         size_t numSubTask = 8;
//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();

//         auto taskCallback = [&](ResultCode ret) {
//             pro.setValue(ret);
//         };
//         mockTask->setCallback(taskCallback);
//         size_t subTaskCalled = 0;
//         auto subTask = [&subTaskCalled]() {
//             ++subTaskCalled;
//             return kvstore::ResultCode::SUCCEEDED;
//         };
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask(subTask);
//         }
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//     }

//     taskMgr->shutdown();
// }

// TEST(TaskManagerTest, gen_sub_task_failed) {
//     auto taskMgr = AdminTaskManager::instance();
//     taskMgr->init();

//     struct MockFailedTask : public AdminTask {
//         ErrOrSubTasks genSubTasks() override {
//             return kvstore::ResultCode::ERR_UNSUPPORTED;
//         }
//     };

//     {
//         std::shared_ptr<AdminTask> mockTask(new MockFailedTask());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();

//         auto cb = [&](ResultCode ret) {
//             pro.setValue(ret);
//         };

//         mockTask->setCallback(cb);
//         taskMgr->addAsyncTask(mockTask);

//         fut.wait();

//         EXPECT_EQ(fut.value(), kvstore::ResultCode::ERR_UNSUPPORTED);
//     }

//     taskMgr->shutdown();
// }

// TEST(TaskManagerTest, some_subtask_failed) {
//     auto taskMgr = AdminTaskManager::instance();
//     taskMgr->init();
//     {
//         size_t numSubTask = 1;
//         size_t subTaskCalled = 0;
//         auto errorCode = kvstore::ResultCode::ERR_PART_NOT_FOUND;

//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = static_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask([&]() {
//                 ++subTaskCalled;
//                 return i == numSubTask / 2 ? suc : errorCode;
//             });
//         }
//         mockTask->setCallback([&](ResultCode ret) {
//             pro.setValue(ret);
//         });
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//         EXPECT_EQ(fut.value(), errorCode);
//     }

//     {
//         size_t numSubTask = 2;
//         size_t subTaskCalled = 0;
//         auto errorCode = kvstore::ResultCode::ERR_LEADER_CHANGED;

//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = static_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask([&]() {
//                 ++subTaskCalled;
//                 return i == numSubTask / 2 ? suc : errorCode;
//             });
//         }
//         mockTask->setCallback([&](ResultCode ret) {
//             pro.setValue(ret);
//         });
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//         EXPECT_EQ(fut.value(), errorCode);
//     }

//     {
//         size_t numSubTask = 4;
//         size_t subTaskCalled = 0;
//         auto errorCode = kvstore::ResultCode::ERR_TAG_NOT_FOUND;

//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = static_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask([&]() {
//                 ++subTaskCalled;
//                 return i == numSubTask / 2 ? suc : errorCode;
//             });
//         }
//         mockTask->setCallback([&](ResultCode ret) {
//             pro.setValue(ret);
//         });
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//         EXPECT_EQ(fut.value(), errorCode);
//     }

//     {
//         size_t numSubTask = 8;
//         size_t subTaskCalled = 0;
//         auto errorCode = kvstore::ResultCode::ERR_UNKNOWN;

//         std::shared_ptr<AdminTask> task(new HookableTask());
//         HookableTask* mockTask = static_cast<HookableTask*>(task.get());
//         folly::Promise<ResultCode> pro;
//         folly::Future<ResultCode> fut = pro.getFuture();
//         for (size_t i = 0; i < numSubTask; ++i) {
//             mockTask->addSubTask([&]() {
//                 ++subTaskCalled;
//                 return i == numSubTask / 2 ? suc : errorCode;
//             });
//         }
//         mockTask->setCallback([&](ResultCode ret) {
//             pro.setValue(ret);
//         });
//         taskMgr->addAsyncTask(task);
//         fut.wait();

//         EXPECT_EQ(numSubTask, subTaskCalled);
//         EXPECT_EQ(fut.value(), errorCode);
//     }

//     taskMgr->shutdown();
// }

TEST(TaskManagerTest, cancel_a_running_task_with_only_1_sub_task) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    size_t numTask = 1;

    std::shared_ptr<AdminTask> task(new HookableTask());
    HookableTask* mockTask = static_cast<HookableTask*>(task.get());
    folly::Promise<ResultCode> taskFinishedPro;
    folly::Future<ResultCode> taskFinished = taskFinishedPro.getFuture();

    folly::Promise<int> taskCancelledPro;
    folly::Future<int> taskCancelledFut = taskCancelledPro.getFuture();

    mockTask->addSubTask([&]() {
        taskCancelledFut.wait();
        usleep(5000);
        return suc;
    });
    mockTask->setCallback([&](ResultCode ret) {
        ++numTask;
        taskFinishedPro.setValue(ret);
    });
    mockTask->setJobId(1);
    taskMgr->addAsyncTask(task);
    taskMgr->cancelTask(1);
    taskCancelledPro.setValue(0);
    taskFinished.wait();

    taskMgr->shutdown();
    EXPECT_EQ(taskCancelledFut.value(), suc);
}

TEST(TaskManagerTest, cancel_1_task_in_a_2_tasks_queue) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();

    auto err = kvstore::ResultCode::ERR_LEADER_CHANGED;
    {
        // add 2 task into queue
        // cancel task1
        // check 1: task0 suc
        // check 2: task1 err
        folly::Promise<ResultCode> task0_pro;
        folly::Future<ResultCode> task0_val = task0_pro.getFuture();

        folly::Promise<ResultCode> task1_pro;
        folly::Future<ResultCode> task1_val = task1_pro.getFuture();

        folly::Promise<int> task1_cancle_pro;
        folly::Future<int> task1_cancel = task1_cancle_pro.getFuture();

        std::shared_ptr<AdminTask> vtask0(new HookableTask());
        HookableTask* task0 = static_cast<HookableTask*>(vtask0.get());
        task0->setJobId(0);

        task0->addSubTask([&]() {
            task1_cancel.wait();
            std::this_thread::sleep_for(100ms);
            return suc;
        });
        task0->setCallback([&](ResultCode ret) {
            task0_pro.setValue(ret);
        });

        std::shared_ptr<AdminTask> vtask1(new HookableTask());
        HookableTask* task1 = static_cast<HookableTask*>(vtask1.get());
        task1->setJobId(1);

        task1->addSubTask([&]() {
            return suc;
        });
        task1->setCallback([&](ResultCode ret) {
            task1_pro.setValue(ret);
        });

        taskMgr->addAsyncTask(vtask0);
        taskMgr->addAsyncTask(vtask1);

        taskMgr->cancelTask(1);
        task1_cancle_pro.setValue(0);

        // task0_val.wait();
        task1_val.wait();

        EXPECT_EQ(task0_val.value(), suc);
        EXPECT_EQ(task1_val.value(), err);
    }

    taskMgr->shutdown();
}

TEST(TaskManagerTest, cancel_a_running_task_with_2_sub_task) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    auto err = kvstore::ResultCode::ERR_LEADER_CHANGED;

    folly::Promise<ResultCode> task0_pro;
    folly::Future<ResultCode> task0_val = task0_pro.getFuture();

    folly::Promise<int> cancle_pro;
    folly::Future<int> cancel = cancle_pro.getFuture();

    std::shared_ptr<AdminTask> vtask0(new HookableTask());
    HookableTask* task0 = static_cast<HookableTask*>(vtask0.get());
    task0->setJobId(0);

    task0->fGenSubTasks = [&]() {
        // std::this_thread::sleep_for(100ms);
        cancel.wait();
        return task0->subTasks;
    };

    task0->addSubTask([&]() {
        LOG(INFO) << "run task0 subTask()";
        return suc;
    });

    task0->setCallback([&](ResultCode ret) {
        task0_pro.setValue(ret);
    });

    taskMgr->addAsyncTask(vtask0);

    taskMgr->cancelTask(1);
    cancle_pro.setValue(0);

    task0_val.wait();

    EXPECT_EQ(task0_val.value(), err);

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
