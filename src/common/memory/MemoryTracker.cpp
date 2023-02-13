/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/memory/MemoryTracker.h"

namespace nebula {
namespace memory {

thread_local ThreadMemoryStats MemoryStats::threadMemoryStats_;

ThreadMemoryStats::ThreadMemoryStats() : reserved(0) {}

ThreadMemoryStats::~ThreadMemoryStats() {
  // Return to global any reserved bytes on destruction
  if (reserved != 0) {
    MemoryStats::instance().freeGlobal(reserved);
  }
}

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

bool MemoryTracker::isOn() {
  return MemoryStats::instance().throwOnMemoryExceeded();
}

void MemoryTracker::allocImpl(int64_t size, bool throw_if_memory_exceeded) {
  MemoryStats::instance().alloc(size, throw_if_memory_exceeded);
}

}  // namespace memory
}  // namespace nebula
