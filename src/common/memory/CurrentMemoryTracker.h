/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once

#include <atomic>

#include "common/base/Base.h"

/// Convenience methods, that use current thread's memory_tracker if it is available.
struct CurrentMemoryTracker {
  /// Call the following functions before calling of corresponding operations with memory
  /// allocators.
  static void alloc(int64_t size);
  static void allocNoThrow(int64_t size);
  static void realloc(int64_t old_size, int64_t new_size);

  /// This function should be called after memory deallocation.
  static void free(int64_t size);
  static void check();

 private:
  static void allocImpl(int64_t size, bool throw_if_memory_exceeded);
};
