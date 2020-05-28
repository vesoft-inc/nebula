/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/thread/GenericWorker.h"
#include "common/time/Duration.h"

namespace nebula {
namespace thread {

TEST(GenericWorker, StartAndStop) {
    // inactive worker
    {
        GenericWorker worker;
    }
    // start & stop & wait
    {
        GenericWorker worker;
        ASSERT_TRUE(worker.start());
        ASSERT_TRUE(worker.stop());
        ASSERT_TRUE(worker.wait());
    }
    // start & stop & no wait
    {
        GenericWorker worker;
        ASSERT_TRUE(worker.start());
        ASSERT_TRUE(worker.stop());
    }
    // start & no stop & no wait
    {
        GenericWorker worker;
        ASSERT_TRUE(worker.start());
    }
    // start twice
    {
        GenericWorker worker;
        ASSERT_TRUE(worker.start());
        ASSERT_FALSE(worker.start());
    }
    // stop twice
    {
        GenericWorker worker;
        ASSERT_TRUE(worker.start());
        ASSERT_TRUE(worker.stop());
        ASSERT_FALSE(worker.stop());
    }
}

TEST(GenericWorker, addTask) {
    GenericWorker worker;
    ASSERT_TRUE(worker.start());
    // task without parameters
    {
        volatile auto flag = false;
        auto set_flag = [&] () mutable { flag = true; };
        worker.addTask(set_flag).get();
        ASSERT_TRUE(flag);
    }
    // task with parameter
    {
        volatile auto flag = false;
        auto set_flag = [&] (auto value) { flag = value; };
        worker.addTask(set_flag, true).get();
        ASSERT_TRUE(flag);
        worker.addTask(set_flag, false).get();
        ASSERT_FALSE(flag);
    }
    // future with value
    {
        ASSERT_TRUE(worker.addTask([] () { return true; }).get());
        ASSERT_FALSE(worker.addTask([] () { return false; }).get());
        ASSERT_EQ(13UL, worker.addTask([] () { return ::strlen("Rock 'n' Roll"); }).get());
        ASSERT_EQ("Innuendo", worker.addTask([] () { return std::string("Innuendo"); }).get());
    }
    // member function as task
    {
        struct X {
            std::string itos(size_t i) {
                return std::to_string(i);
            }
        } x;
        ASSERT_EQ("918", worker.addTask(&X::itos, &x, 918).get());
        ASSERT_EQ("918", worker.addTask(&X::itos, std::make_shared<X>(), 918).get());
    }
}

static testing::AssertionResult msAboutEqual(size_t expected, size_t actual) {
    if (std::max(expected, actual) - std::min(expected, actual) <= 10) {
        return testing::AssertionSuccess();
    }
    return testing::AssertionFailure() << "actual: " << actual
                                       << ", expected: " << expected;
}

TEST(GenericWorker, addDelayTask) {
    GenericWorker worker;
    ASSERT_TRUE(worker.start());
    {
        auto shared = std::make_shared<int>(0);
        auto cb = [shared] () {
            return ++(*shared);
        };
        time::Duration clock;
        ASSERT_EQ(1, worker.addDelayTask(50, cb).get());
        ASSERT_GE(shared.use_count(), 2);
        ASSERT_TRUE(msAboutEqual(50, clock.elapsedInUSec() / 1000));
        ::usleep(5 * 1000);     // ensure all internal resources are released
        ASSERT_EQ(2, shared.use_count());   // two ref: `shared' and `cb'
    }
}

TEST(GenericWorker, addRepeatTask) {
    GenericWorker worker;
    ASSERT_TRUE(worker.start());
    {
        auto counter = 0UL;
        auto cb = [&] () {
            counter++;
        };
        worker.addRepeatTask(50, cb);
        ::usleep(160 * 1000);
        ASSERT_EQ(3, counter);
    }
}

TEST(GenericWorker, purgeRepeatTask) {
    GenericWorker worker;
    ASSERT_TRUE(worker.start());
    {
        auto counter = 0UL;
        auto cb = [&] () {
            counter++;
        };
        auto id = worker.addRepeatTask(50, cb);
        // fprintf(stderr, "id: 0x%016lx\n", id);
        ::usleep(110 * 1000);
        worker.purgeTimerTask(id);
        ::usleep(50 * 1000);
        ASSERT_EQ(2, counter);
    }
}

}   // namespace thread
}   // namespace nebula
