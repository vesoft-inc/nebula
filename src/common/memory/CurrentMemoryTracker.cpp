/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/memory/CurrentMemoryTracker.h"

#include "common/memory/MemoryStats.h"
#include "common/memory/MemoryUtils.h"

namespace DB {
namespace ErrorCodes {
extern const int LOGICAL_ERROR;
}
}  // namespace DB

namespace {

// MemoryTracker* getMemoryTracker() {
//   if (auto* thread_memory_tracker = DB::CurrentThread::getMemoryTracker())
//     return thread_memory_tracker;
//
//   /// Once the main thread is initialized,
//   /// total_memory_tracker is initialized too.
//   /// And can be used, since MainThreadStatus is required for profiling.
//   if (DB::MainThreadStatus::get()) return &total_memory_tracker;
//
//   return nullptr;
// }

}  // namespace

// using DB::current_thread;

void CurrentMemoryTracker::allocImpl(int64_t size, bool) {
  int64_t used = nebula::MemoryStats::instance().amount();
  int64_t limit = nebula::MemoryUtils::kMemoryLimit;
  if (used + size > limit) {
    throw std::bad_alloc();
  }
  nebula::MemoryStats::instance().add(size);
}

void CurrentMemoryTracker::check() {}

void CurrentMemoryTracker::alloc(int64_t size) {
  bool throw_if_memory_exceeded = true;
  allocImpl(size, throw_if_memory_exceeded);
}

void CurrentMemoryTracker::allocNoThrow(int64_t size) {
  bool throw_if_memory_exceeded = false;
  allocImpl(size, throw_if_memory_exceeded);
}

void CurrentMemoryTracker::realloc(int64_t old_size, int64_t new_size) {
  int64_t addition = new_size - old_size;
  addition > 0 ? alloc(addition) : free(-addition);
}

void CurrentMemoryTracker::free(int64_t size) {
  nebula::MemoryStats::instance().add(-size);
}
