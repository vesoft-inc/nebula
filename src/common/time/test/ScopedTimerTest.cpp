/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>  // for TestPartResult
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartResult
#include <stdint.h>       // for int64_t, uint64_t
#include <unistd.h>       // for usleep

#include <functional>  // for function
#include <memory>      // for allocator

#include "common/time/ScopedTimer.h"  // for ScopedTimer, SCOPED_TIMER

namespace nebula {

TEST(ScopedTimerTest, TestScopedTimer) {
  int64_t var, var2;
  {
    var = 0;
    SCOPED_TIMER(&var);
    ::usleep(100);
  }
  EXPECT_GE(var, 99);

  {
    var = 0;
    var2 = 0;
    SCOPED_TIMER(&var);
    SCOPED_TIMER(&var2);
    ::usleep(1);
  }
  EXPECT_GE(var, var2);

  {
    var = 0;
    ScopedTimer st([&](uint64_t et) { var += et; });
    ::usleep(10);
  }
  EXPECT_GE(var, 9);

  {
    var = 0;
    ScopedTimer st(&var, true);
    st.start();
    ::usleep(10);
    st.stop();
    EXPECT_GE(var, 9);
  }
}

}  // namespace nebula
