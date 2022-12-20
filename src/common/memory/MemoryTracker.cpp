/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/memory/MemoryTracker.h"

namespace nebula {
namespace memory {

thread_local int64_t MemoryStats::localReserved_{0};

void MemoryTracker::alloc(int64_t size) {
  bool throw_if_memory_exceeded = true;
  allocImpl(size, throw_if_memory_exceeded);
}

void MemoryTracker::allocNoThrow(int64_t size) {
  bool throw_if_memory_exceeded = false;
  allocImpl(size, throw_if_memory_exceeded);
}

void MemoryTracker::realloc(int64_t old_size, int64_t new_size) {
  int64_t addition = new_size - old_size;
  addition > 0 ? alloc(addition) : free(-addition);
}

void MemoryTracker::free(int64_t size) {
  MemoryStats::instance().free(size);
}

void MemoryTracker::allocImpl(int64_t size, bool) {
  MemoryStats::instance().alloc(size);
}

}  // namespace memory
}  // namespace nebula
