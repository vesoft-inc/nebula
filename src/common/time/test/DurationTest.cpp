/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "time/Duration.h"
#include "vector"
#include "cmath"
#include "time/WallClock.h"

using nebula::time::Duration;
using nebula::time::WallClock;
TEST(Duration, elapsedInSeconds) {
    for (int i = 0; i < 5; i++) {
        Duration dur;
        auto start = std::chrono::steady_clock::now();
        sleep(2);
        auto diff = std::chrono::steady_clock::now() - start;
        dur.pause();

        ASSERT_EQ(std::chrono::duration_cast<std::chrono::seconds>(diff).count(),
                  dur.elapsedInSec()) << "Inaccuracy in iteration " << i;
    }
}


TEST(Duration, elapsedInMilliSeconds) {
    Duration dur;
    for (int i = 0; i < 200; i++) {
        dur.reset();
        auto start = std::chrono::steady_clock::now();
        usleep(5000);   // Sleep for 5 ms
        auto diff = std::chrono::steady_clock::now() - start;
        dur.pause();

        // Allow 1ms difference
        ASSERT_LE(std::chrono::duration_cast<std::chrono::milliseconds>(diff).count(),
                  dur.elapsedInMSec()) << "Inaccuracy in iteration " << i;
        ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() + 1,
                  dur.elapsedInMSec()) << "Inaccuracy in iteration " << i;
    }
}
TEST(WallClock , time ) {
    std::vector<int64_t> time1;
    std::vector<int64_t> time2;
    for (uint32_t i = 0; i < 10; i++) {
        auto sec1 = WallClock::fastNowInMicroSec();
        usleep(3000);
        auto sec2 = WallClock::fastNowInMicroSec();
        auto cost_time = sec2-sec1;
        time1.push_back(cost_time);
    }
    std::cout << time1[0] << std::endl;
    int64_t sum = std::accumulate(time1.begin(), time1.end(), 0);
    double average = sum*1.0/10;
    std::cout << "Mean Error  : " << average-3000 << "ms " <<std::endl;

    for (int i = 0; i < 10 ; i++) {
        int64_t a = (time1[i]-average)*(time1[i]-average);
        time2.push_back(a);
    }
    int64_t sum2 = std::accumulate(time2.begin(), time2.end(), 0);
    double variance = sum2*1.0/9;
    std::cout << "The  Variance  Is : "<< variance << std::endl;
    double error = (abs(average-3000) * 1.0 / 3000) * 100;
    std :: cout << "Test Error Is  : " << error  << "%" << std::endl;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

