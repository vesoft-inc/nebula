/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once

#include <jemalloc/jemalloc.h>

#include <new>

#include "common/base/Base.h"
#include "common/memory/CurrentMemoryTracker.h"

namespace Memory {

inline ALWAYS_INLINE size_t alignToSizeT(std::align_val_t align) noexcept {
  return static_cast<size_t>(align);
}

inline ALWAYS_INLINE void* newImpl(std::size_t size) {
  void* ptr = malloc(size);

  if (LIKELY(ptr != nullptr)) return ptr;

  /// @note no std::get_new_handler logic implemented
  throw std::bad_alloc{};
}

inline ALWAYS_INLINE void* newImpl(std::size_t size, std::align_val_t align) {
  void* ptr = aligned_alloc(alignToSizeT(align), size);

  if (LIKELY(ptr != nullptr)) return ptr;

  /// @note no std::get_new_handler logic implemented
  throw std::bad_alloc{};
}

inline ALWAYS_INLINE void* newNoExept(std::size_t size) noexcept {
  return malloc(size);
}

inline ALWAYS_INLINE void* newNoExept(std::size_t size, std::align_val_t align) noexcept {
  return aligned_alloc(static_cast<size_t>(align), size);
}

inline ALWAYS_INLINE void deleteImpl(void* ptr) noexcept {
  free(ptr);
}

inline ALWAYS_INLINE void deleteSized(void* ptr, std::size_t size) noexcept {
  if (UNLIKELY(ptr == nullptr)) return;
  sdallocx(ptr, size, 0);
}

inline ALWAYS_INLINE void deleteSized(void* ptr,
                                      std::size_t size,
                                      std::align_val_t align) noexcept {
  if (UNLIKELY(ptr == nullptr)) return;
  sdallocx(ptr, size, MALLOCX_ALIGN(alignToSizeT(align)));
}

#include <malloc.h>

inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size) {
  size_t actual_size = size;

  /// The nallocx() function allocates no memory, but it performs the same size computation as the
  /// mallocx() function
  /// @note je_mallocx() != je_malloc(). It's expected they don't differ much in allocation logic.
  if (LIKELY(size != 0)) {
    actual_size = nallocx(size, 0);
  }
  return actual_size;
}
inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size,
                                                    std::align_val_t align[[maybe_unused]]) {
  size_t actual_size = size;
  /// The nallocx() function allocates no memory, but it performs the same size computation as the
  /// mallocx() function
  /// @note je_mallocx() != je_malloc(). It's expected they don't differ much in allocation logic.
  if (LIKELY(size != 0)) {
    actual_size = nallocx(size, MALLOCX_ALIGN(alignToSizeT(align)));
  }

  return actual_size;
}

inline ALWAYS_INLINE void trackMemory(std::size_t size) {
  std::size_t actual_size = getActualAllocationSize(size);
  CurrentMemoryTracker::allocNoThrow(actual_size);
}

inline ALWAYS_INLINE void trackMemory(std::size_t size, std::align_val_t align) {
  std::size_t actual_size = getActualAllocationSize(size, align);
  CurrentMemoryTracker::allocNoThrow(actual_size);
}

inline ALWAYS_INLINE void untrackMemory(void* ptr) noexcept {
  try {
    /// @note It's also possible to use je_malloc_usable_size() here.
    if (LIKELY(ptr != nullptr)) {
      CurrentMemoryTracker::free(sallocx(ptr, 0));
    }
  } catch (...) {
  }
}

inline ALWAYS_INLINE void untrackMemory(void* ptr[[maybe_unused]],
                                        std::size_t size[[maybe_unused]],
                                        std::align_val_t align
                                        [[maybe_unused]] = std::align_val_t(8)) noexcept {
  try {
    /// @note It's also possible to use je_malloc_usable_size() here.
    if (LIKELY(ptr != nullptr)) {
      CurrentMemoryTracker::free(sallocx(ptr, MALLOCX_ALIGN(alignToSizeT(align))));
    }
  } catch (...) {
  }
}

}  // namespace Memory
