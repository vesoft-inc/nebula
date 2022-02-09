/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/thread/NamedThread.h"

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
