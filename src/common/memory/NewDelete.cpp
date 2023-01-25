/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <new>

#include "common/memory/Memory.h"
/// Replace default new/delete with memory tracking versions.
/// ENABLE_MEMORY_TRACKER is defined by cmake only following two condition satisfied:
///   1. jemalloc is used
///      MemoryTracker need jemalloc API to get accurate size of alloc/free memory.
///   2. address_sanitizer is off
///      sanitizer has already override the new/delete operator,
///      only override new/delete operator only when address_sanitizer is off

#ifdef ENABLE_MEMORY_TRACKER
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
  nebula::memory::trackMemoryNoThrow(size);
  return nebula::memory::newNoException(size);
}

void *operator new[](std::size_t size, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemoryNoThrow(size);
  return nebula::memory::newNoException(size);
}

void *operator new(std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemoryNoThrow(size, align);
  return nebula::memory::newNoException(size, align);
}

void *operator new[](std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemoryNoThrow(size, align);
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

#else
#pragma message("WARNING: MemoryTracker was disabled at compile time")
#endif
