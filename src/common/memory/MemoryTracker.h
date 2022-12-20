/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once

#include <atomic>

#include "common/base/Base.h"

namespace nebula {
namespace memory {

// Memory Stats enable record memory usage at thread local scope and global scope.
// Design:
//    Thread: Each thread has a reserved memory quota got from global. Each time alloc() or free()
//    occurs, it first tries request size from local reserved.
//
//    Global: Counting the global used memory, the actually used memory may less than the counted
//    usage, since threads have quota reservation.
class MemoryStats {
 public:
  static MemoryStats& instance() {
    static MemoryStats stats;
    return stats;
  }

  inline ALWAYS_INLINE void alloc(int64_t size) {
    int64_t willBe = localReserved_ - size;

    if UNLIKELY (willBe < 0) {
      // Calculate how many bytes needed to get from global
      int64_t getFromGlobal = kLocalReservedLimit_;
      while (willBe + getFromGlobal <= 0) {
        getFromGlobal += kLocalReservedLimit_;
      }
      // allocGlobal() may end with bad_alloc, only invoke allocGlobal() once (ALL_OR_NOTHING
      // semantic)
      allocGlobal(getFromGlobal);
      willBe += getFromGlobal;
    }
    // Only update after successful allocations, failed allocations should not be taken into
    // account.
    localReserved_ = willBe;
  }

  inline ALWAYS_INLINE void free(int64_t size) {
    localReserved_ += size;
    // Return if local reserved exceed limit
    while (localReserved_ > kLocalReservedLimit_) {
      // freeGlobal() never fail, can be invoked multiple times.
      freeGlobal(kLocalReservedLimit_);
      localReserved_ -= kLocalReservedLimit_;
    }
  }

  void setLimit(int64_t limit) {
    this->limit_ = limit;
  }

  int64_t getLimit() {
    return this->limit_;
  }

  int64_t used() {
    return used_;
  }

  double usedRatio() {
    return used_ / static_cast<double>(limit_);
  }

 private:
  inline ALWAYS_INLINE void allocGlobal(int64_t size) {
    int64_t willBe = size + used_.fetch_add(size, std::memory_order_relaxed);
    if (willBe > limit_) {
      // revert
      used_.fetch_sub(size, std::memory_order_relaxed);
      throw std::bad_alloc();
    }
  }

  inline ALWAYS_INLINE void freeGlobal(int64_t bytes) {
    used_.fetch_sub(bytes, std::memory_order_relaxed);
  }

 private:
  // Global
  int64_t limit_{std::numeric_limits<int64_t>::max()};
  std::atomic<int64_t> used_{0};
  // Local
  static thread_local int64_t localReserved_;
  static constexpr int64_t kLocalReservedLimit_ = 1 * 1024 * 1024;
};

// A memory tracker enable tracking every memory allocation and free.
// This is not where real memory allocation or free happens, it only do the tracking staff.
struct MemoryTracker {
  /// Call the following functions before calling of corresponding operations with memory
  /// allocators.
  static void alloc(int64_t size);
  static void allocNoThrow(int64_t size);
  static void realloc(int64_t old_size, int64_t new_size);

  /// This function should be called after memory deallocation.
  static void free(int64_t size);

 private:
  static void allocImpl(int64_t size, bool throw_if_memory_exceeded);
};

}  // namespace memory
}  // namespace nebula
