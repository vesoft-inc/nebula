/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "time/Duration.h"
#include "time/WallClock.h"
#include "base/Base.h"
#include <gtest/gtest.h>
#include<vector>
#include<math.h>

 using nebula::time:: WallClock;

TEST(WallClock , time ) {
    std::vector<int64_t> timer;
    std::vector<int64_t>s;

    for (uint32_t i = 0; i < 10; i++) {
        auto sec = WallClock::fastNowInMicroSec();
        usleep(3000);
        auto sec2 = WallClock::fastNowInMicroSec();
        auto cost_time = sec2-sec;
        timer.push_back(cost_time);
    }
    std::cout << timer[0] << std::endl;
    int64_t sum = std::accumulate(timer.begin() , timer.end() , 0);
    std :: cout << WallClock::getCurrentTimeStr() <<std::endl;
    double average = sum*1.0/10;
    std::cout << "Mean Error  : " << average-3000 << "ms "<< std::endl;

    for (int i = 0; i < 10; i++) {
        int64_t a = (timer[i] - average) * (timer[i] - average);
        s.push_back(a);
    }
    int64_t sum2 = std::accumulate(s.begin() , s.end() , 0);
    double variance = sum2*1.0/9;
    std::cout << "The  Variance  Is : "<< variance << std::endl;
    double error = (abs(average-3000) * 1.0 / 3000) * 100;
    std :: cout << "Test Fast  Error Is  : " << error  << "%" << std::endl;
}

TEST(WallClock , time1 ) {
    std::vector<int64_t> timer;
    std::vector<int64_t>s;

    for (uint32_t i = 0; i < 10; i++) {
        auto sec = WallClock::slowNowInMicroSec();
        usleep(3000);
        auto sec2 = WallClock::slowNowInMicroSec();
        auto cost_time = sec2-sec;
        timer.push_back(cost_time);
    }
    std::cout << timer[0] << std::endl;
    int64_t sum = std::accumulate(timer.begin() , timer.end() , 0);
    std::cout << WallClock::getCurrentTimeStr() << std::endl;
    double average = sum*1.0/10;
    std::cout << "Mean Error  : " << average-3000 << "ms "<< std::endl;
    for (int i = 0; i < 10 ; i++) {
        int64_t a = (timer[i] - average)*(timer[i] - average);
        s.push_back(a);
    }
    int64_t sum2 = std::accumulate(s.begin() , s.end() , 0);
    double variance = sum2*1.0/9;
    std::cout << "The  Variance  Is : "<< variance << std::endl;
    double error = (abs(average-3000) * 1.0 / 3000) * 100;
    std :: cout << "Test Slow  Error Is  : " << error  << "%" << std::endl;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
