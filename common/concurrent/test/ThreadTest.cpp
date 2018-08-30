/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include <gtest/gtest.h>
#include "concurrent/thread/NamedThread.h"

namespace vesoft {
namespace concurrent {

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
    pid_t tid;
    auto getter = [&] () {
        tid = ::syscall(SYS_gettid);
    };

    NamedThread thread("", getter);
    thread.join();
    ASSERT_EQ(tid, thread.tid());
}

}   // namespace concurrent
}   // namespace vesoft
