/* Copyright (c) 2026 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <new>

#include "common/memory/MemoryTracker.h"

namespace nebula {
namespace memory {

// The overridden operator new only exists when the tracker is compiled in (it is off in ASAN
// builds, where a request this large would also abort the process instead of failing).
#ifdef ENABLE_MEMORY_TRACKER

// jemalloc can size this request (nallocx() returns a valid size class), but cannot satisfy it.
static constexpr std::size_t kImpossibleSize = std::size_t{1} << 56;

// A failed allocation may still move one thread-local reservation chunk between this thread and
// the global counter, so allow that much; a leaked count would be ~2^56 bytes.
static void expectUsedUnchanged(int64_t before) {
  EXPECT_LE(std::abs(MemoryStats::instance().used() - before), MiB);
}

TEST(MemoryTrackerTest, FailedNewDoesNotLeakCount) {
  void* ptr = nullptr;
  int64_t before = MemoryStats::instance().used();
  EXPECT_THROW(ptr = ::operator new(kImpossibleSize), std::bad_alloc);
  expectUsedUnchanged(before);

  before = MemoryStats::instance().used();
  EXPECT_THROW(ptr = ::operator new[](kImpossibleSize), std::bad_alloc);
  expectUsedUnchanged(before);

  before = MemoryStats::instance().used();
  EXPECT_THROW(ptr = ::operator new (kImpossibleSize, std::align_val_t{64}), std::bad_alloc);
  expectUsedUnchanged(before);
  EXPECT_EQ(nullptr, ptr);
}

TEST(MemoryTrackerTest, FailedNothrowNewDoesNotLeakCount) {
  int64_t before = MemoryStats::instance().used();
  EXPECT_EQ(nullptr, ::operator new(kImpossibleSize, std::nothrow));
  expectUsedUnchanged(before);

  before = MemoryStats::instance().used();
  EXPECT_EQ(nullptr, ::operator new[](kImpossibleSize, std::nothrow));
  expectUsedUnchanged(before);

  before = MemoryStats::instance().used();
  EXPECT_EQ(nullptr, ::operator new (kImpossibleSize, std::align_val_t{64}, std::nothrow));
  expectUsedUnchanged(before);
}

#else

// Keeps the binary linkable (gtest_main needs a test) and reports the skip.
TEST(MemoryTrackerTest, DISABLED_RequiresMemoryTracker) {}

#endif

}  // namespace memory
}  // namespace nebula
