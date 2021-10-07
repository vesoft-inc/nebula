/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/ThreadLocal.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/runtime/MemoryTracker.h"

DEFINE_int32(num_mem_trackers_per_thread, 1024, "Number of memory trackers per thread");

using ThreadLocalMemoryTracker = folly::ThreadLocal<nebula::MemoryTracker>;

namespace nebula {

class DummyQueryContext {
 public:
  DummyQueryContext() = default;

 private:
  ThreadLocalMemoryTracker memTrackers_;
};

TEST(MemoryTrackerTest, TestThreadLocal) {
  auto dummy = new DummyQueryContext[FLAGS_num_mem_trackers_per_thread];
  UNUSED(dummy);
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
