/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>  // for TestPartResult
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartResult
#include <unistd.h>       // for getpid

#include <string>  // for allocator, string

#include "common/thread/NamedThread.h"  // for NamedThread, gettid, NamedThr...

namespace nebula {
namespace thread {

TEST(NamedThread, ThreadName) {
  std::string setname("thread");
  std::string getname;
  auto getter = [&]() { NamedThread::Nominator::get(getname); };
  NamedThread thread(setname, getter);
  thread.join();
  ASSERT_EQ(setname, getname);
}

TEST(NamedThread, ThreadID) {
  ASSERT_EQ(::getpid(), nebula::thread::gettid());
}

}  // namespace thread
}  // namespace nebula
