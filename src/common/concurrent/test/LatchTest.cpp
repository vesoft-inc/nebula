/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/concurrent/Latch.h"
#include "common/thread/GenericThreadPool.h"

namespace nebula {
namespace concurrent {

TEST(LatchTest, BasicTest) {
    // test for invalid initial counter
    {
        ASSERT_THROW({Latch latch(0);}, std::invalid_argument);
    }
    // test for illegal `downWait'
    {
        ASSERT_THROW({
            Latch latch(1);
            latch.down();
            latch.downWait();
        }, std::runtime_error);
    }
    // test for illegal `down'
    {
        ASSERT_THROW({
            Latch latch(1);
            latch.down();
            latch.down();
        }, std::runtime_error);
    }
    // test for single-thread normal case
    {
        Latch latch(1);
        ASSERT_FALSE(latch.isReady());
        latch.down();
        ASSERT_TRUE(latch.isReady());
        latch.wait();
        latch.wait();
        ASSERT_TRUE(true);
    }
    // test for multiple-thread normal case
    {
        Latch latch(2);
        auto cb = [&] () {
            latch.downWait();
        };
        std::thread thread(cb);
        ASSERT_FALSE(latch.isReady());
        latch.downWait();
        ASSERT_TRUE(latch.isReady());
        thread.join();
    }
}

TEST(LatchTest, JoinLikeTest) {
    // start bunch of tasks, then wait for them all done.
    constexpr auto nthreads = 4UL;
    constexpr auto ntasks = 16UL;
    thread::GenericThreadPool pool;
    Latch latch(ntasks);
    std::atomic<size_t> counter{0};
    auto task = [&] () {
        ++counter;
        latch.down();
    };
    pool.start(nthreads);
    for (auto i = 0UL; i < ntasks; i++) {
        pool.addTask(task);
    }
    latch.wait();
    ASSERT_EQ(ntasks, counter.load());
}

TEST(LatchTest, SignalTest) {
    // There are preceding I/O works and subsequent CPU bound works.
    // Do I/O works with single thread, and CPU works concurrently.
    constexpr auto nthreads = 16UL;
    constexpr auto ntasks = 16UL;
    thread::GenericThreadPool pool;
    Latch latch(1);
    pool.start(nthreads);
    std::atomic<size_t> counter{0};
    auto task = [&] () {
        // do some preparing works
        latch.wait();   // wait for the I/O works done
        // do subsequent CPU bound works, where parallelism is more efficient.
        ++counter;
    };
    for (auto i = 0UL; i < ntasks; i++) {
        pool.addTask(task);
    }
    // sleep to simulate I/O bound task, which single threading suffices
    usleep(100000);
    // I/O works done
    ASSERT_EQ(0, counter.load());   // no CPU bound work done.
    latch.down();
    pool.stop();
    pool.wait();    // wait all tasks done
    ASSERT_EQ(ntasks, counter.load());  // all tasks are done
}

}   // namespace concurrent
}   // namespace nebula
