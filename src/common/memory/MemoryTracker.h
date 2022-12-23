/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once

#include <atomic>

#include "common/base/Base.h"

namespace nebula {
namespace memory {

// Memory stats for each thread.
struct ThreadMemoryStats {
  ThreadMemoryStats();
  ~ThreadMemoryStats();

  // reserved bytes size in current thread
  int64_t reserved;
};

/**
 *  Memory Stats enable record memory usage at thread local scope and global scope.
 *  Design:
 *    It is a singleton instance, designed for Thread scope and Global scope
 *    memory quota counting.
 *
 *    Thread: Each thread has a reserved memory quota got from global. Each time alloc() or free()
 *            occurs, it first tries request size from local reserved.
 *    Global: Counting the global used memory, the actually used memory may less than the counted
 *            usage, since threads have quota reservation.
 */
class MemoryStats {
 public:
  static MemoryStats& instance() {
    static MemoryStats stats;
    return stats;
  }

  /// Inform size of memory allocation
  inline ALWAYS_INLINE void alloc(int64_t size) {
    int64_t willBe = threadMemoryStats_.reserved - size;

    if UNLIKELY (willBe < 0) {
      // if local reserved is not enough, calculate how many bytes needed to get from global.
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
    threadMemoryStats_.reserved = willBe;
  }

  /// Inform size of memory deallocation
  inline ALWAYS_INLINE void free(int64_t size) {
    threadMemoryStats_.reserved += size;
    // Return if local reserved exceed limit
    while (threadMemoryStats_.reserved > kLocalReservedLimit_) {
      // freeGlobal() never fail, can be invoked multiple times.
      freeGlobal(kLocalReservedLimit_);
      threadMemoryStats_.reserved -= kLocalReservedLimit_;
    }
  }

  /// Free global memory, two user case may call this function:
  /// 1. free()
  /// 2. destruction of ThreadMemoryStats return reserved memory
  inline ALWAYS_INLINE void freeGlobal(int64_t bytes) {
    used_.fetch_sub(bytes, std::memory_order_relaxed);
  }

  /// Set limit (maximum usable bytes) of memory
  void setLimit(int64_t limit) {
    if (this->limit_ != limit) {
      this->limit_ = limit;
    }
  }

  /// Get limit (maximum usable bytes) of memory
  int64_t getLimit() {
    return this->limit_;
  }

  /// Get current used bytes of memory
  int64_t used() {
    return used_;
  }

  /// Calculate used ratio of memory
  double usedRatio() {
    return used_ / static_cast<double>(limit_);
  }

  std::string toString() {
    return fmt::format("MemoryStats, limit:{}, used:{}", limit_, used_);
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

 private:
  // Global
  int64_t limit_{std::numeric_limits<int64_t>::max()};
  std::atomic<int64_t> used_{0};
  // Thread Local
  static thread_local ThreadMemoryStats threadMemoryStats_;
  // Each thread reserves this amount of memory
  static constexpr int64_t kLocalReservedLimit_ = 1 * 1024 * 1024;
};

// A global static memory tracker enable tracking every memory allocation and deallocation.
// This is not the place where real memory allocation or deallocation happens, only do the
// memory tracking.
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
