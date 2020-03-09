/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/admin/AdminTaskManager.h"

namespace nebula {
namespace storage {

using ResultCode = nebula::kvstore::ResultCode;
using TaskHandle = std::pair<int, int>;

class MockAdminTask : public AdminTask {
public:
    using ResultCode = nebula::kvstore::ResultCode;
    using RetSubTask = ErrorOr<nebula::kvstore::ResultCode, std::vector<AdminSubTask>>;

    explicit MockAdminTask(nebula::kvstore::ResultCode rc) : rc_(rc) {}
    ResultCode run() override {
        return rc_;
    }

    ResultCode stop() override {
        return rc_;
    }

    RetSubTask genSubTasks() override {
        return nebula::kvstore::ResultCode::SUCCEEDED;
    }

private:
    ResultCode rc_;
    std::function<void()> onFinished_;
};

struct MockCallBack {
    void operator()(ResultCode rc) {
        UNUSED(rc);
    }
};

TEST(TaskManagerTest, ctor) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    taskMgr->shutdown();
}

TEST(TaskManagerTest, happy_path) {
    auto taskMgr = AdminTaskManager::instance();
    taskMgr->init();
    taskMgr->shutdown_ = false;

    TaskHandle handle = std::make_pair(100, 100);
    {
        auto rc = ResultCode::SUCCEEDED;
        std::shared_ptr<AdminTask> mockTask(new MockAdminTask(rc));
        MockCallBack cb;
        taskMgr->addAsyncTask2(handle, mockTask, cb);
    }

    taskMgr->shutdown();
}

// TEST(TaskManagerTest, cancelTask) {
//     auto taskMgr = AdminTaskManager::instance();
//     taskMgr->init();
//     taskMgr->shutdown_ = false;
//     TaskHandle handle = std::make_pair(100, 100);
//     auto suc = ResultCode::SUCCEEDED;
//     std::shared_ptr<AdminTask> mockTask(new MockAdminTask(suc));

//     auto result = taskMgr->addAsyncTask(handle, mockTask);
//     result.wait();
//     EXPECT_EQ(result.value(), suc);
//     taskMgr->shutdown();
// }

// TEST(TaskManagerTest, no_currency) {
//     auto taskMgr = AdminTaskManager::instance();
//     taskMgr->init();
//     taskMgr->shutdown_ = false;

//     TaskHandle handle = std::make_pair(100, 100);

//     {
//         auto rc = ResultCode::SUCCEEDED;
//         std::shared_ptr<AdminTask> mockTask(new MockAdminTask(rc));
//         auto result = taskMgr->addAsyncTask(handle, mockTask);
//         result.wait();
//         EXPECT_EQ(result.value(), rc);
//     }

//     {
//         auto rc = ResultCode::ERR_SPACE_NOT_FOUND;
//         std::shared_ptr<AdminTask> mockTask(new MockAdminTask(rc));
//         auto result = taskMgr->addAsyncTask(handle, mockTask);
//         result.wait();
//         EXPECT_EQ(result.value(), rc);
//     }

//     {
//         auto rc = ResultCode::ERR_PART_NOT_FOUND;
//         std::shared_ptr<AdminTask> mockTask(new MockAdminTask(rc));
//         auto result = taskMgr->addAsyncTask(handle, mockTask);
//         result.wait();
//         EXPECT_EQ(result.value(), rc);
//     }

//     {
//         auto rc = ResultCode::ERR_KEY_NOT_FOUND;
//         std::shared_ptr<AdminTask> mockTask(new MockAdminTask(rc));
//         auto result = taskMgr->addAsyncTask(handle, mockTask);
//         result.wait();
//         EXPECT_EQ(result.value(), rc);
//     }

//     taskMgr->shutdown();
// }

// TEST(TaskManagerTest, cancelTask) {
//     auto taskMgr = AdminTaskManager::instance();
//     taskMgr->init();
//     taskMgr->shutdown_ = false;
//     TaskHandle handle = std::make_pair(100, 100);
//     auto suc = ResultCode::SUCCEEDED;
//     std::shared_ptr<AdminTask> mockTask(new MockAdminTask(suc));

//     auto result = taskMgr->addAsyncTask(handle, mockTask);
//     result.wait();
//     EXPECT_EQ(result.value(), suc);
//     taskMgr->shutdown();
// }

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
