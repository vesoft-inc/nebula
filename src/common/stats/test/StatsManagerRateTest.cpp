/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/stats/StatsManager.h"
#include "common/thread/GenericWorker.h"

namespace nebula {
namespace stats {

TEST(StatsManager, RateTest) {
    auto statId = StatsManager::registerStats("ratetest", "AVG, SUM");
    auto thread = std::make_unique<thread::GenericWorker>();
    ASSERT_TRUE(thread->start());

    auto task = [&statId] () {
        StatsManager::addValue(statId);
    };
    constexpr auto qps = 100L;
    thread->addRepeatTask(1 * 1000 / qps, task);

    ::usleep(60 * 1000 * 1000);

    auto actual = StatsManager::readValue("ratetest.rate.60").value();

    ASSERT_LT(std::max(qps, actual) - std::min(qps, actual), 10L) << "expected: " << qps
                                                                  << ", actual: " << actual;

    thread->stop();
    thread->wait();
    thread.reset();
}

}   // namespace stats
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
