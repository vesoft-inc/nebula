/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "stats/StatsManager.h"

namespace nebula {
namespace stats {

TEST(StatsManager, StatsTest) {
    auto statId = StatsManager::registerStats("stat01");
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([statId, i] () {
            for (int k = i * 10 + 1; k <= i * 10 + 10; k++) {
                StatsManager::addValue(statId, k);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Let's check
    EXPECT_EQ(5050, StatsManager::readValue("stat01.sum.60"));
    EXPECT_EQ(5050, StatsManager::readValue("stat01.SUM.600"));
    EXPECT_EQ(5050, StatsManager::readValue("stat01.Sum.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat01.Sum.300"));
    EXPECT_EQ(100, StatsManager::readValue("stat01.count.60"));
    EXPECT_EQ(100, StatsManager::readValue("stat01.COUNT.600"));
    EXPECT_EQ(100, StatsManager::readValue("stat01.Count.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat01.Count.4600"));
    EXPECT_EQ(50, StatsManager::readValue("stat01.avg.60"));
    EXPECT_EQ(50, StatsManager::readValue("stat01.AVG.600"));
    EXPECT_EQ(50, StatsManager::readValue("stat01.Avg.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat01.Avg1.3600"));
}


TEST(StatsManager, HistogramTest) {
    auto statId = StatsManager::registerHisto("stat02", 1, 1, 100);
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([statId, i] () {
            for (int k = i * 10 + 1; k <= i * 10 + 10; k++) {
                StatsManager::addValue(statId, k);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Let's check
    EXPECT_EQ(5050, StatsManager::readValue("stat02.sum.60"));
    EXPECT_EQ(5050, StatsManager::readValue("stat02.SUM.600"));
    EXPECT_EQ(5050, StatsManager::readValue("stat02.Sum.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat02.Sum.300"));
    EXPECT_EQ(100, StatsManager::readValue("stat02.count.60"));
    EXPECT_EQ(100, StatsManager::readValue("stat02.COUNT.600"));
    EXPECT_EQ(100, StatsManager::readValue("stat02.Count.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat02.Count.4600"));
    EXPECT_EQ(50, StatsManager::readValue("stat02.avg.60"));
    EXPECT_EQ(50, StatsManager::readValue("stat02.AVG.600"));
    EXPECT_EQ(50, StatsManager::readValue("stat02.Avg.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat02.Avg1.3600"));

    EXPECT_EQ(100, StatsManager::readValue("stat02.p99.60"));
    EXPECT_EQ(100, StatsManager::readValue("stat02.P99.600"));
    EXPECT_EQ(100, StatsManager::readValue("stat02.p99.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat02.t99.60"));
    EXPECT_EQ(96, StatsManager::readValue("stat02.p9599.60"));
    EXPECT_EQ(96, StatsManager::readValue("stat02.P9599.600"));
    EXPECT_EQ(96, StatsManager::readValue("stat02.p9599.3600"));
    EXPECT_EQ(0, StatsManager::readValue("stat02.t9599.60"));
}


TEST(StatsManager, CrossLevelTest) {
    auto statId = StatsManager::registerHisto("stat03", 1, 1, 100);
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([statId, i] () {
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
    EXPECT_EQ(4500, StatsManager::readValue("stat03.sum.60"));
    EXPECT_EQ(5050, StatsManager::readValue("stat03.SUM.600"));
    EXPECT_EQ(90, StatsManager::readValue("stat03.count.60"));
    EXPECT_EQ(100, StatsManager::readValue("stat03.COUNT.600"));
    EXPECT_EQ(99, StatsManager::readValue("stat03.p99.60"));
    EXPECT_EQ(100, StatsManager::readValue("stat03.P99.600"));
}

}   // namespace stats
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

