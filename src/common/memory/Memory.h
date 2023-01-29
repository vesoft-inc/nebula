/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once

#if ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include <new>

#include "common/base/Base.h"
#include "common/memory/MemoryTracker.h"

namespace nebula {
namespace memory {

inline ALWAYS_INLINE size_t alignToSizeT(std::align_val_t align) noexcept {
  return static_cast<size_t>(align);
}

inline ALWAYS_INLINE void* newImpl(std::size_t size) {
  void* ptr = malloc(size);

  if (LIKELY(ptr != nullptr)) return ptr;

  throw std::bad_alloc{};
}

inline ALWAYS_INLINE void* newImpl(std::size_t size, std::align_val_t align) {
  void* ptr = aligned_alloc(alignToSizeT(align), size);

  if (LIKELY(ptr != nullptr)) return ptr;

  throw std::bad_alloc{};
}

inline ALWAYS_INLINE void* newNoException(std::size_t size) noexcept {
  return malloc(size);
}

inline ALWAYS_INLINE void* newNoException(std::size_t size, std::align_val_t align) noexcept {
  return aligned_alloc(static_cast<size_t>(align), size);
}

inline ALWAYS_INLINE void deleteImpl(void* ptr) noexcept {
  free(ptr);
}

#if ENABLE_JEMALLOC
inline ALWAYS_INLINE void deleteSized(void* ptr, std::size_t size) noexcept {
  if (UNLIKELY(ptr == nullptr)) return;
  sdallocx(ptr, size, 0);
}
#else
inline ALWAYS_INLINE void deleteSized(void* ptr, std::size_t size) noexcept {
  UNUSED(size);
  free(ptr);
}
#endif

#if ENABLE_JEMALLOC
inline ALWAYS_INLINE void deleteSized(void* ptr,
                                      std::size_t size,
                                      std::align_val_t align) noexcept {
  if (UNLIKELY(ptr == nullptr)) return;
  sdallocx(ptr, size, MALLOCX_ALIGN(alignToSizeT(align)));
}
#else
inline ALWAYS_INLINE void deleteSized(void* ptr,
                                      std::size_t size,
                                      std::align_val_t align) noexcept {
  UNUSED(size);
  UNUSED(align);
  free(ptr);
}
#endif

inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size) {
  size_t actual_size = size;

#if ENABLE_JEMALLOC
  // The nallocx() function allocates no memory,
  // but it performs the same size computation as the mallocx() function
  if (LIKELY(size != 0)) {
    actual_size = nallocx(size, 0);
  }
#endif
  return actual_size;
}
inline ALWAYS_INLINE size_t getActualAllocationSize(size_t size, std::align_val_t align) {
  size_t actual_size = size;

#if ENABLE_JEMALLOC
  // The nallocx() function allocates no memory,
  // but it performs the same size computation as the mallocx() function
  if (LIKELY(size != 0)) {
    actual_size = nallocx(size, MALLOCX_ALIGN(alignToSizeT(align)));
  }
#else
  UNUSED(align);
#endif
  return actual_size;
}

inline ALWAYS_INLINE void trackMemory(std::size_t size) {
  std::size_t actual_size = getActualAllocationSize(size);
  MemoryTracker::alloc(actual_size);
}

inline ALWAYS_INLINE void trackMemoryNoThrow(std::size_t size) {
  std::size_t actual_size = getActualAllocationSize(size);
  MemoryTracker::allocNoThrow(actual_size);
}

inline ALWAYS_INLINE void trackMemory(std::size_t size, std::align_val_t align) {
  std::size_t actual_size = getActualAllocationSize(size, align);
  MemoryTracker::alloc(actual_size);
}

inline ALWAYS_INLINE void trackMemoryNoThrow(std::size_t size, std::align_val_t align) {
  std::size_t actual_size = getActualAllocationSize(size, align);
  MemoryTracker::allocNoThrow(actual_size);
}

inline ALWAYS_INLINE void untrackMemory(void* ptr) noexcept {
  try {
#if ENABLE_JEMALLOC
    if (LIKELY(ptr != nullptr)) {
      MemoryTracker::free(sallocx(ptr, 0));
    }
#else
    // malloc_usable_size() result may greater or equal to allocated size.
    MemoryTracker::free(malloc_usable_size(ptr));
#endif
  } catch (...) {
  }
}

inline ALWAYS_INLINE void untrackMemory(void* ptr, std::size_t size) noexcept {
  try {
#if ENABLE_JEMALLOC
    UNUSED(size);
    if (LIKELY(ptr != nullptr)) {
      MemoryTracker::free(sallocx(ptr, 0));
    }
#else
    if (size) {
      MemoryTracker::free(size);
    }
    // malloc_usable_size() result may greater or equal to allocated size.
    MemoryTracker::free(malloc_usable_size(ptr));
#endif
  } catch (...) {
  }
}

inline ALWAYS_INLINE void untrackMemory(void* ptr,
                                        std::size_t size,
                                        std::align_val_t align) noexcept {
  try {
#if ENABLE_JEMALLOC
    UNUSED(size);
    if (LIKELY(ptr != nullptr)) {
      MemoryTracker::free(sallocx(ptr, MALLOCX_ALIGN(alignToSizeT(align))));
    }
#else
    UNUSED(align);
    if (size) {
      MemoryTracker::free(size);
    } else {
      // malloc_usable_size() result may greater or equal to allocated size.
      MemoryTracker::free(malloc_usable_size(ptr));
    }
#endif
  } catch (...) {
  }
}

}  // namespace memory
}  // namespace nebula
