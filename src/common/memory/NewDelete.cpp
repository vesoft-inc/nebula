/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <new>

#include "common/memory/Memory.h"
/// Replace default new/delete with memory tracking versions.
///
/// Two condition need check before MemoryTracker is on
///   1. jemalloc is used
///      MemoryTracker need jemalloc API to get accurate size of alloc/free memory.
#if ENABLE_JEMALLOC
///   2. address_sanitizer is off
///      sanitizer has already override the new/delete operator,
///      only override new/delete operator only when address_sanitizer is off
#if defined(__clang)
#if defined(__has_feature)
#if not __has_feature(address_sanitizer)
#define ENABLE_MEMORY_TRACKER
#endif
#endif

#else  // gcc
#define ENABLE_MEMORY_TRACKER
#endif
#endif

#if defined(ENABLE_MEMORY_TRACKER)
/// new
void *operator new(std::size_t size) {
  nebula::memory::trackMemory(size);
  return nebula::memory::newImpl(size);
}

void *operator new(std::size_t size, std::align_val_t align) {
  nebula::memory::trackMemory(size, align);
  return nebula::memory::newImpl(size, align);
}

void *operator new[](std::size_t size) {
  nebula::memory::trackMemory(size);
  return nebula::memory::newImpl(size);
}

void *operator new[](std::size_t size, std::align_val_t align) {
  nebula::memory::trackMemory(size, align);
  return nebula::memory::newImpl(size, align);
}

void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemory(size);
  return nebula::memory::newNoException(size);
}

void *operator new[](std::size_t size, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemory(size);
  return nebula::memory::newNoException(size);
}

void *operator new(std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemory(size, align);
  return nebula::memory::newNoException(size, align);
}

void *operator new[](std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemory(size, align);
  return nebula::memory::newNoException(size, align);
}

/// delete
void operator delete(void *ptr) noexcept {
  nebula::memory::untrackMemory(ptr);
  nebula::memory::deleteImpl(ptr);
}

void operator delete(void *ptr, std::align_val_t align) noexcept {
  nebula::memory::untrackMemory(ptr, 0, align);
  nebula::memory::deleteImpl(ptr);
}

void operator delete[](void *ptr) noexcept {
  nebula::memory::untrackMemory(ptr);
  nebula::memory::deleteImpl(ptr);
}

void operator delete[](void *ptr, std::align_val_t align) noexcept {
  nebula::memory::untrackMemory(ptr, 0, align);
  nebula::memory::deleteImpl(ptr);
}

void operator delete(void *ptr, std::size_t size) noexcept {
  nebula::memory::untrackMemory(ptr, size);
  nebula::memory::deleteSized(ptr, size);
}

void operator delete(void *ptr, std::size_t size, std::align_val_t align) noexcept {
  nebula::memory::untrackMemory(ptr, size, align);
  nebula::memory::deleteSized(ptr, size, align);
}

void operator delete[](void *ptr, std::size_t size) noexcept {
  nebula::memory::untrackMemory(ptr, size);
  nebula::memory::deleteSized(ptr, size);
}

void operator delete[](void *ptr, std::size_t size, std::align_val_t align) noexcept {
  nebula::memory::untrackMemory(ptr, size, align);
  nebula::memory::deleteSized(ptr, size, align);
}

#endif
