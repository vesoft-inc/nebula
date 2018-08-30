/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include <gtest/gtest.h>
#include <atomic>
#include "concurrent/thread/ThreadLocalPtr.h"
#include "concurrent/sync/Barrier.h"

namespace vesoft {
namespace concurrent {

TEST(ThreadLocalPtr, SingleThread) {
    ThreadLocalPtr<std::string> tls([](auto *ptr){ delete ptr; });
    auto cb = [&] () {
        ASSERT_EQ(nullptr, tls.get());
        tls.reset(new std::string("Bohemian Rhapsody"));
        ASSERT_EQ("Bohemian Rhapsody", *tls.get());
        tls.reset();
        ASSERT_EQ(nullptr, tls.get());
    };
    std::thread thread(cb);
    thread.join();
}

TEST(ThreadLocalPtr, MultiThread) {
    ThreadLocalPtr<int> tls([](auto *ptr){ delete ptr; });
    auto cb = [&] (int idx) {
        ASSERT_EQ(nullptr, tls.get());
        tls.reset(new int(idx));
        ASSERT_EQ(idx, *tls.get());
        ASSERT_EQ(idx, *tls);
        tls.reset();
        ASSERT_EQ(nullptr, tls.get());
    };
    std::vector<std::thread> threads;
    for (auto i = 0; i < 128; i++) {
        threads.emplace_back(cb, i);
    }

    for (auto &th : threads) {
        th.join();
    }
}

TEST(ThreadLocalPtr, Operators) {
    struct Object { int i{0xAA}; };
    ThreadLocalPtr<Object> tls([](auto *ptr) { delete ptr; });

    ASSERT_FALSE(static_cast<bool>(tls));
    ASSERT_TRUE(!tls);
    ASSERT_FALSE(!!tls);
    ASSERT_TRUE(tls == nullptr);
    ASSERT_TRUE(nullptr == tls);

    tls.reset(new Object);

    ASSERT_TRUE(static_cast<bool>(tls));
    ASSERT_FALSE(!tls);
    ASSERT_TRUE(!!tls);
    ASSERT_TRUE(nullptr != tls);
    ASSERT_TRUE(tls != nullptr);
    ASSERT_EQ(0xAA, tls->i);
    ASSERT_EQ(0xAA, (*tls).i);

    tls.reset();

    ASSERT_FALSE(static_cast<bool>(tls));
    ASSERT_TRUE(!tls);
    ASSERT_FALSE(!!tls);
    ASSERT_TRUE(tls == nullptr);
    ASSERT_TRUE(nullptr == tls);
}

class Object {
public:
    Object() { ++cnt_; }
    ~Object() { --cnt_; }
    static std::atomic<size_t> cnt_;
};
std::atomic<size_t> Object::cnt_{0};

TEST(ThreadLocalPtr, Destructor) {
    constexpr auto N = 128;
    auto completion = [=] () {
        ASSERT_EQ(N, Object::cnt_.load());
    };

    concurrent::Barrier barrier(N, completion);

    ThreadLocalPtr<Object> tls([](auto *ptr) { delete ptr; });
    auto cb = [&] () {
        ASSERT_EQ(nullptr, tls.get());
        tls.reset(new Object);
        ASSERT_NE(nullptr, tls.get());
        barrier.wait();
    };
    std::vector<std::thread> threads;
    for (auto i = 0; i < 128; i++) {
        threads.emplace_back(cb);
    }

    for (auto &th : threads) {
        th.join();
    }

    ASSERT_EQ(0UL, Object::cnt_.load());
}


}   // namespace concurrent
}   // namespace vesoft
