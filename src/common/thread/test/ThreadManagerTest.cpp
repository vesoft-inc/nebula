/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "thread/ThreadManager.h"

namespace nebula {
namespace thread {

TEST(ThreadManagerTest, GenericTests) {
    using namespace apache::thrift::concurrency;
    {
        FLAGS_num_threads_per_priority = "";
        FLAGS_num_worker_threads = 1;
        std::shared_ptr<ThreadManager> workers(nebula::thread::getThreadManager());
        ASSERT_NE(nullptr, workers);
        ASSERT_EQ(typeid(*workers), typeid(SimpleThreadManager<folly::LifoSem>));
    }

    {
        FLAGS_num_threads_per_priority = "0:1:2:2";
        std::shared_ptr<ThreadManager> workers(nebula::thread::getThreadManager());
        ASSERT_NE(nullptr, workers);
        ASSERT_EQ(typeid(*workers), typeid(PriorityThreadManager::PriorityImplT<folly::LifoSem>));
        std::shared_ptr<PriorityThreadManager> ptm =
                std::dynamic_pointer_cast<PriorityThreadManager>(workers);
        int8_t count = ptm->workerCount(apache::thrift::concurrency::HIGH_IMPORTANT);
        EXPECT_EQ(0, count);
        count = ptm->workerCount(apache::thrift::concurrency::HIGH);
        EXPECT_EQ(1, count);
        count = ptm->workerCount(apache::thrift::concurrency::IMPORTANT);
        EXPECT_EQ(2, count);
        count = ptm->workerCount(apache::thrift::concurrency::NORMAL);
        EXPECT_EQ(2, count);
        count = ptm->workerCount(apache::thrift::concurrency::BEST_EFFORT);
        EXPECT_EQ(0, count);
    }
}

}   // namespace thread
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
