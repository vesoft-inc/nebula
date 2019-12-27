/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "time/Duration.h"
#include <vector>

using nebula::time::Duration;

volatile int sink;
TEST(Duration, elapsedInSeconds) {
    for (int size = 1; sizze < 100000001; size *= 100) {
        Duration dur;
        auto start = std::chrono::steady_clock::now();
	std::vector<int> v(size, 42);
        sink = std::accumulate(v.begin(),v.end(),7);
        std::chrono::duration<double> diff = std::chrono::steady_clock::now() - start;
        dur.pause();

        std::cout << "Time to fill and iterate a vector of " << size << " ints : " << diff.count() << " s\n";
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


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

