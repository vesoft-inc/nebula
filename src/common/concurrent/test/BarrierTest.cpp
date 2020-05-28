/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/concurrent/Barrier.h"
#include "common/thread/GenericThreadPool.h"

namespace nebula {
namespace concurrent {

TEST(BarrierTest, BasicTest) {
    // test for invalid initial counter
    {
        ASSERT_THROW({Barrier barrier(0UL);}, std::invalid_argument);
    }
    // test for single-thread normal case
    {
        Barrier barrier(1UL);
        barrier.wait();
        ASSERT_TRUE(true);
    }
    // test for multiple-thread normal case
    {
        Barrier barrier(2UL);
        std::atomic<size_t> counter{0};
        auto cb = [&] () {
            barrier.wait();
            ++counter;
        };
        std::thread thread(cb);
        usleep(1000);
        ASSERT_EQ(0UL, counter.load());
        barrier.wait();
        thread.join();
        ASSERT_EQ(1UL, counter.load());
    }
    // test for multiple-thread completion
    {
        std::atomic<size_t> counter{0};
        auto completion = [&] () {
            ++counter;
            ++counter;
        };
        Barrier barrier(2UL, completion);

        auto cb = [&] () {
            barrier.wait();
            ++counter;
        };

        std::thread thread(cb);
        usleep(1000);
        ASSERT_EQ(0UL, counter.load());
        barrier.wait();
        ASSERT_GE(counter.load(), 2UL);
        thread.join();
        ASSERT_EQ(3UL, counter.load());
    }
}

TEST(BarrierTest, ConsecutiveTest) {
    std::atomic<size_t> counter{0};
    constexpr auto N = 64UL;
    constexpr auto iters = 100UL;
    auto completion = [&] () {
        // At the completion phase, `counter' should be multiple to `N'.
        ASSERT_EQ(0UL, counter.load() % N);
    };

    Barrier barrier(N, completion);
    auto cb = [&] () {
        auto i = iters;
        while (i-- != 0) {
            ++counter;
            barrier.wait();
        }
    };

    std::vector<std::thread> threads;
    for (auto i = 0UL; i < N; i++) {
        threads.emplace_back(cb);
    }
    for (auto &thread : threads) {
        thread.join();
    }
    ASSERT_EQ(0UL, counter.load() % N);
}

}   // namespace concurrent
}   // namespace nebula
