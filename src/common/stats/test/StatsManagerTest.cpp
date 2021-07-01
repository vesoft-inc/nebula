/* Copyright (c) 2018 vesoft inc. All rights reserved.
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

TEST(StatsManager, StatsTest) {
    auto statId = StatsManager::registerStats("stat01", "");
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&statId, i] () {
            for (int k = i * 10 + 1; k <= i * 10 + 10; k++) {
                sleep(6);
                StatsManager::addValue(statId, k);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Let's check
    EXPECT_EQ(550, StatsManager::readValue("stat01.sum.5").value());
    EXPECT_EQ(5050, StatsManager::readValue("stat01.sum.60").value());
    EXPECT_EQ(5050, StatsManager::readValue("stat01.SUM.600").value());
    EXPECT_EQ(5050, StatsManager::readValue("stat01.Sum.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat01.Sum.300").ok());
    EXPECT_EQ(10, StatsManager::readValue("stat01.count.5").value());
    EXPECT_EQ(100, StatsManager::readValue("stat01.count.60").value());
    EXPECT_EQ(100, StatsManager::readValue("stat01.COUNT.600").value());
    EXPECT_EQ(100, StatsManager::readValue("stat01.Count.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat01.Count.4600").ok());
    EXPECT_EQ(55, StatsManager::readValue("stat01.avg.5").value());
    EXPECT_EQ(50, StatsManager::readValue("stat01.avg.60").value());
    EXPECT_EQ(50, StatsManager::readValue("stat01.AVG.600").value());
    EXPECT_EQ(50, StatsManager::readValue("stat01.Avg.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat01.Avg1.3600").ok());
}


TEST(StatsManager, HistogramTest) {
    auto statId = StatsManager::registerHisto("stat02", 1, 1, 100, "");
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&statId, i] () {
            for (int k = i * 10 + 1; k <= i * 10 + 10; k++) {
                sleep(6);
                StatsManager::addValue(statId, k);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Let's check
    EXPECT_EQ(550, StatsManager::readValue("stat02.sum.5").value());
    EXPECT_EQ(5050, StatsManager::readValue("stat02.sum.60").value());
    EXPECT_EQ(5050, StatsManager::readValue("stat02.SUM.600").value());
    EXPECT_EQ(5050, StatsManager::readValue("stat02.Sum.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat02.Sum.300").ok());
    EXPECT_EQ(10, StatsManager::readValue("stat02.count.5").value());
    EXPECT_EQ(100, StatsManager::readValue("stat02.count.60").value());
    EXPECT_EQ(100, StatsManager::readValue("stat02.COUNT.600").value());
    EXPECT_EQ(100, StatsManager::readValue("stat02.Count.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat02.Count.4600").ok());
    EXPECT_EQ(55, StatsManager::readValue("stat02.avg.5").value());
    EXPECT_EQ(50, StatsManager::readValue("stat02.avg.60").value());
    EXPECT_EQ(50, StatsManager::readValue("stat02.AVG.600").value());
    EXPECT_EQ(50, StatsManager::readValue("stat02.Avg.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat02.Avg1.3600").ok());

    EXPECT_EQ(100, StatsManager::readValue("stat02.p99.60").value());
    EXPECT_EQ(100, StatsManager::readValue("stat02.P99.600").value());
    EXPECT_EQ(100, StatsManager::readValue("stat02.p99.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat02.t99.60").ok());
    EXPECT_EQ(96, StatsManager::readValue("stat02.p9599.60").value());
    EXPECT_EQ(96, StatsManager::readValue("stat02.P9599.600").value());
    EXPECT_EQ(96, StatsManager::readValue("stat02.p9599.3600").value());
    EXPECT_FALSE(StatsManager::readValue("stat02.t9599.60").ok());
}


bool counterExists(const folly::dynamic& stats, const std::string& counter, int64_t& val) {
    for (auto& stat : stats) {
        if (stat.isObject() && stat.count(counter)) {
            val = stat[counter].asInt();
            return true;
        }
    }
    return false;
}


TEST(StatsManager, ReadAllTest) {
    auto statId1 = StatsManager::registerStats("stat03", "RATE, sum");
    auto statId2 = StatsManager::registerHisto("stat04", 1, 1, 100, "sum, p95, p99");
    StatsManager::addValue(statId1, 1);
    StatsManager::addValue(statId1, 2);
    StatsManager::addValue(statId2, 1);
    StatsManager::addValue(statId2, 2);

    auto stats = folly::dynamic::array();
    StatsManager::readAllValue(stats);

    int64_t val;
    EXPECT_TRUE(counterExists(stats, "stat04.sum.5", val));
    EXPECT_EQ(3, val);
    EXPECT_TRUE(counterExists(stats, "stat04.p95.5", val));
    EXPECT_EQ(2, val);
    EXPECT_TRUE(counterExists(stats, "stat04.p99.3600", val));
    EXPECT_EQ(2, val);

    EXPECT_TRUE(counterExists(stats, "stat03.sum.3600", val));
    EXPECT_EQ(3, val);

    EXPECT_FALSE(counterExists(stats, "stat03.count.600", val));
    EXPECT_FALSE(counterExists(stats, "stat03.avg.3600", val));

    EXPECT_FALSE(counterExists(stats, "stat04.rate.600", val));
    EXPECT_FALSE(counterExists(stats, "stat04.count.60", val));
    EXPECT_FALSE(counterExists(stats, "stat04.avg.3600", val));
    EXPECT_FALSE(counterExists(stats, "stat04.p75.5", val));
}

}   // namespace stats
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
