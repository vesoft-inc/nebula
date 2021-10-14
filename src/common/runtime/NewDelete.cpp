/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <jemalloc/jemalloc.h>

#include <new>

#include "common/base/Base.h"
#include "common/runtime/MemoryTracker.h"
#include "common/thread/ThreadState.h"

using nebula::thread::kCurrentThreadState;

namespace {

void *newImpl(size_t bytes) {
  auto *ptr = malloc(bytes);
  if (UNLIKELY(ptr == nullptr)) {
    throw std::bad_alloc{};
  }
  return ptr;
}

void *newImpl(size_t bytes, std::align_val_t align) {
  auto *ptr = aligned_alloc(static_cast<size_t>(align), bytes);
  if (UNLIKELY(ptr == nullptr)) {
    throw std::bad_alloc{};
  }
  return ptr;
}

void *newNoExcept(size_t bytes) noexcept { return malloc(bytes); }

void *newNoExcept(size_t bytes, std::align_val_t align) noexcept {
  return aligned_alloc(bytes, static_cast<size_t>(align));
}

void deleteImpl(void *ptr) noexcept { dallocx(ptr, 0); }

void deleteImpl(void *ptr, std::align_val_t align) noexcept {
  dallocx(ptr, MALLOCX_ALIGN(align));  // NOLINT
}

void deleteSized(void *ptr, size_t bytes) noexcept {
  if (LIKELY(ptr != nullptr)) {
    sdallocx(ptr, bytes, 0);
  }
}

void deleteSized(void *ptr, size_t bytes, std::align_val_t align) noexcept {
  if (LIKELY(ptr != nullptr)) {
    sdallocx(ptr, bytes, MALLOCX_ALIGN(align));  // NOLINT
  }
}

//============= Track Memory ==============

void trackMemory(size_t bytes) {
  if (LIKELY(bytes != 0)) {
    bytes = nallocx(bytes, 0);
  }
  if (kCurrentThreadState->memTracker) {
    kCurrentThreadState->memTracker->consume(bytes);
  }
}

void trackMemory(size_t bytes, std::align_val_t align) {
  if (LIKELY(bytes != 0)) {
    bytes = nallocx(bytes, MALLOCX_ALIGN(align));  // NOLINT
  }
  if (kCurrentThreadState->memTracker) {
    kCurrentThreadState->memTracker->consume(bytes);
  }
}

bool trackMemoryNoExcept(size_t bytes) {
  try {
    trackMemory(bytes);
  } catch (...) {
    return false;
  }

  return true;
}

bool trackMemoryNoExcept(size_t bytes, std::align_val_t align) {
  try {
    trackMemory(bytes, align);
  } catch (...) {
    return false;
  }

  return true;
}

void untrackMemory(MAYBE_UNUSED void *ptr, MAYBE_UNUSED size_t bytes = 0) noexcept {
  try {
    if (LIKELY(ptr != nullptr)) {
      kCurrentThreadState->memTracker->release(sallocx(ptr, 0));
    }
  } catch (...) {
  }
}

void untrackMemory(void *ptr, std::align_val_t align, MAYBE_UNUSED size_t bytes = 0) noexcept {
  try {
    if (LIKELY(ptr != nullptr)) {
      kCurrentThreadState->memTracker->release(sallocx(ptr, MALLOCX_ALIGN(align)));  // NOLINT
    }
  } catch (...) {
  }
}

}  // namespace

void *operator new(size_t bytes) {
  trackMemory(bytes);
  return newImpl(bytes);
}

void *operator new[](size_t bytes) {
  trackMemory(bytes);
  return newImpl(bytes);
}

void *operator new(size_t bytes, std::align_val_t align) {
  trackMemory(bytes, align);
  return newImpl(bytes, align);
}

void *operator new[](size_t bytes, std::align_val_t align) {
  trackMemory(bytes, align);
  return newImpl(bytes, align);
}

void *operator new(size_t bytes, const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(trackMemoryNoExcept(bytes))) {
    return newNoExcept(bytes);
  }
  return nullptr;
}

void *operator new[](size_t bytes, const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(trackMemoryNoExcept(bytes))) {
    return newNoExcept(bytes);
  }
  return nullptr;
}

void *operator new(size_t bytes,
                   std::align_val_t align,
                   const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(trackMemoryNoExcept(bytes, align))) {
    return newNoExcept(bytes, align);
  }
  return nullptr;
}

void *operator new[](size_t bytes,
                     std::align_val_t align,
                     const std::nothrow_t & /*unused*/) noexcept {
  if (LIKELY(trackMemoryNoExcept(bytes, align))) {
    return newNoExcept(bytes, align);
  }
  return nullptr;
}

void operator delete(void *ptr) noexcept {
  untrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete[](void *ptr) noexcept {
  untrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete(void *ptr, std::align_val_t align) noexcept {
  untrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

void operator delete[](void *ptr, std::align_val_t align) noexcept {
  untrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

void operator delete(void *ptr, size_t bytes) noexcept {
  untrackMemory(ptr, bytes);
  deleteSized(ptr, bytes);
}

void operator delete[](void *ptr, size_t bytes) noexcept {
  untrackMemory(ptr, bytes);
  deleteSized(ptr, bytes);
}

void operator delete(void *ptr, size_t bytes, std::align_val_t align) noexcept {
  untrackMemory(ptr, align, bytes);
  deleteSized(ptr, bytes, align);
}

void operator delete[](void *ptr, size_t bytes, std::align_val_t align) noexcept {
  untrackMemory(ptr, align, bytes);
  deleteSized(ptr, bytes, align);
}

void operator delete(void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  untrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete[](void *ptr, const std::nothrow_t & /*unused*/) noexcept {
  untrackMemory(ptr);
  deleteImpl(ptr);
}

void operator delete(void *ptr,
                     std::align_val_t align,
                     const std::nothrow_t & /*unused*/) noexcept {
  untrackMemory(ptr, align);
  deleteImpl(ptr, align);
}

void operator delete[](void *ptr,
                       std::align_val_t align,
                       const std::nothrow_t & /*unused*/) noexcept {
  untrackMemory(ptr, align);
  deleteImpl(ptr, align);
}
