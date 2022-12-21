/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <new>

#include "common/memory/Memory.h"

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#define MEMORY_SANITIZER 1
#endif
#endif

#ifndef MEMORY_SANITIZER
/// Replace default new/delete with memory tracking versions.
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
  return nebula::memory::newNoExept(size);
}

void *operator new[](std::size_t size, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemory(size);
  return nebula::memory::newNoExept(size);
}

void *operator new(std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemory(size, align);
  return nebula::memory::newNoExept(size, align);
}

void *operator new[](std::size_t size, std::align_val_t align, const std::nothrow_t &) noexcept {
  nebula::memory::trackMemory(size, align);
  return nebula::memory::newNoExept(size, align);
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
