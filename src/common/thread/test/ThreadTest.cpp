/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "iostream"
#include "base/Base.h"
#include <gtest/gtest.h>
#include "thread/NamedThread.h"
#include "vector"
namespace nebula {
namespace thread {

TEST(NamedThread, ThreadName) {
    std::string setname("thread");
    std::string getname;
    auto getter = [&] () {
        NamedThread::Nominator::get(getname);
    };
    NamedThread thread(setname, getter);
    thread.join();
    ASSERT_EQ(setname, getname);
}

TEST(NamedThread, ThreadID) {
    ASSERT_EQ(::getpid(), nebula::thread::gettid());
}
TEST(NameThread, test) {
   static int j = 0;
   std::vector <std::thread>t;
   for (int i = 0; i < 10; i++) {
     std::thread mythread(j++);
     t.push_back(std::move(mythread));
   }

   for (int i = 0; i < 10 ; i++) {
   t[i].join();
}
     ASSERT_GE(j, 10);
}
}   // namespace thread
}   // namespace nebula
