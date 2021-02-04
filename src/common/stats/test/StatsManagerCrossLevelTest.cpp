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

TEST(StatsManager, CrossLevelTest) {
    auto statId = StatsManager::registerHisto("stat03", 1, 1, 100, "");
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&statId, i] () {
            for (int k = i * 10 + 10; k >= i * 10 + 1; k--) {
                StatsManager::addValue(statId, k);
                if (k > i * 10 + 1) {
                    sleep(7);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // The first number of each thread should be moved to the next level
    EXPECT_EQ(4500, StatsManager::readValue("stat03.sum.60").value());
    EXPECT_EQ(460, StatsManager::readValue("stat03.sum.5").value());
    EXPECT_EQ(5050, StatsManager::readValue("stat03.SUM.600").value());
    EXPECT_EQ(90, StatsManager::readValue("stat03.count.60").value());
    EXPECT_EQ(10, StatsManager::readValue("stat03.count.5").value());
    EXPECT_EQ(100, StatsManager::readValue("stat03.COUNT.600").value());
    EXPECT_EQ(99, StatsManager::readValue("stat03.p99.60").value());
    EXPECT_EQ(100, StatsManager::readValue("stat03.P99.600").value());
}

}   // namespace stats
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
