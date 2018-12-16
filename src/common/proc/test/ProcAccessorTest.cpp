/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "proc/ProcAccessor.h"

using namespace nebula::proc;

TEST(ProcAccessor, AccessDir) {
    auto stopped = false;
    std::thread t([&] () {
        while (!stopped) {
            usleep(1000);
        }
    });

    ProcAccessor accessor("/proc/self/task");
    size_t nTh = 0;
    std::string tid;
    while (accessor.next(tid)) {
        nTh++;
    }
    EXPECT_EQ(2, nTh);

    stopped = true;
    t.join();
}


TEST(ProcAccessor, AccessFile) {
    std::regex regex("([0-9]+\\.[0-9]{2})[ \\t]+"
                     "([0-9]+\\.[0-9]{2})[ \\t]+"
                     "([0-9]+\\.[0-9]{2})[ \\t]+"
                     "([0-9]+)/[0-9]+");
    std::smatch sm;
    ProcAccessor accessor("/proc/loadavg");

    EXPECT_TRUE(accessor.next(regex, sm));

    double load1 = atof(sm[1].str().c_str());
    double load5 = atof(sm[2].str().c_str());
    double load15 = atof(sm[3].str().c_str());
    int running = atoi(sm[4].str().c_str());

    EXPECT_GE(load1, 0.0);
    EXPECT_GE(load5, 0.0);
    EXPECT_GE(load15, 0.0);
    EXPECT_GE(running, 1);
}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

