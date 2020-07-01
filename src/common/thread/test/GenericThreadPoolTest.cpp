/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/thread/GenericThreadPool.h"
#include "common/time/Duration.h"

namespace nebula {
namespace thread {

TEST(GenericThreadPool, StartAndStop) {
    // inactive pool
    {
        GenericThreadPool pool;
    }
    // start & stop & wait
    {
        GenericThreadPool pool;
        ASSERT_TRUE(pool.start(1));
        ASSERT_TRUE(pool.stop());
        ASSERT_TRUE(pool.wait());
    }
    // start & stop & no wait
    {
        GenericThreadPool pool;
        ASSERT_TRUE(pool.start(1));
        ASSERT_TRUE(pool.stop());
    }
    // start & no stop & no wait
    {
        GenericThreadPool pool;
        ASSERT_TRUE(pool.start(1));
    }
    // start twice
    {
        GenericThreadPool pool;
        ASSERT_TRUE(pool.start(1));
        ASSERT_FALSE(pool.start(1));
    }
    // stop twice
    {
        GenericThreadPool pool;
        ASSERT_TRUE(pool.start(1));
        ASSERT_TRUE(pool.stop());
        ASSERT_FALSE(pool.stop());
    }
}

TEST(GenericThreadPool, addTask) {
    GenericThreadPool pool;
    ASSERT_TRUE(pool.start(1));
    // task without parameters
    {
        volatile auto flag = false;
        auto set_flag = [&] () { flag = true; };
        pool.addTask(set_flag).get();
        ASSERT_TRUE(flag);
    }
    // task with parameter
    {
        volatile auto flag = false;
        auto set_flag = [&] (auto value) { flag = value; };
        pool.addTask(set_flag, true).get();
        ASSERT_TRUE(flag);
        pool.addTask(set_flag, false).get();
        ASSERT_FALSE(flag);
    }
    // future with value
    {
        ASSERT_TRUE(pool.addTask([] () { return true; }).get());
        ASSERT_FALSE(pool.addTask([] () { return false; }).get());
        ASSERT_EQ(13UL, pool.addTask([] () { return ::strlen("Rock 'n' Roll"); }).get());
        ASSERT_EQ("Innuendo", pool.addTask([] () { return std::string("Innuendo"); }).get());
    }
    // member function as task
    {
        struct X {
            std::string itos(size_t i) {
                return std::to_string(i);
            }
        } x;
        ASSERT_EQ("918", pool.addTask(&X::itos, &x, 918).get());
        ASSERT_EQ("918", pool.addTask(&X::itos, std::make_shared<X>(), 918).get());
    }
}

static testing::AssertionResult msAboutEqual(size_t expected, size_t actual) {
    if (std::max(expected, actual) - std::min(expected, actual) <= 10) {
        return testing::AssertionSuccess();
    }
    return testing::AssertionFailure() << "actual: " << actual
                                       << ", expected: " << expected;
}

TEST(GenericThreadPool, addDelayTask) {
    GenericThreadPool pool;
    ASSERT_TRUE(pool.start(1));
    {
        auto shared = std::make_shared<int>(0);
        auto cb = [shared] () {
            return ++(*shared);
        };
        time::Duration clock;
        ASSERT_EQ(1, pool.addDelayTask(50, cb).get());
        ASSERT_GE(shared.use_count(), 2);
        ASSERT_TRUE(msAboutEqual(50, clock.elapsedInUSec() / 1000));
        ::usleep(5 * 1000);     // ensure all internal resources are released
        ASSERT_EQ(2, shared.use_count());   // two ref: `shared' and `cb'
    }
}

TEST(GenericThreadPool, addRepeatTask) {
    GenericThreadPool pool;
    ASSERT_TRUE(pool.start(1));
    {
        auto counter = 0UL;
        auto cb = [&] () {
            counter++;
        };
        pool.addRepeatTask(50, cb);
        ::usleep(160 * 1000);
        ASSERT_EQ(3, counter);
    }
}

TEST(GenericThreadPool, DISABLED_purgeRepeatTask) {
    GenericThreadPool pool;
    ASSERT_TRUE(pool.start(4));
    for (auto i = 0; i < 8; i++) {
        auto counter = 0UL;
        auto cb = [&] () {
            counter++;
        };
        auto id = pool.addRepeatTask(50, cb);
        // fprintf(stderr, "id: 0x%016lx\n", id);
        ::usleep(110 * 1000);
        pool.purgeTimerTask(id);
        ::usleep(50 * 1000);
        ASSERT_EQ(2, counter) << "i: " << i << ", id: " << id;
    }
}

}   // namespace thread
}   // namespace nebula
