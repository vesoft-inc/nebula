// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "common/base/Arena.h"

#include <cstdint>

namespace nebula {

void* Arena::allocateAligned(const std::size_t alloc) {
  DCHECK_NE(alloc, 0);  // don't allow zero sized allocation
  // replace the modulo operation by bit and
  static_assert(kAlignment && !(kAlignment & (kAlignment - 1)), "Align must be power of 2.");
  const std::size_t pad =
      kAlignment - (reinterpret_cast<uintptr_t>(currentPtr_) & (kAlignment - 1));
  const std::size_t consumption = alloc + pad;
  if (UNLIKELY(consumption > kMaxChunkSize)) {
    DLOG(FATAL) << "Arena can't allocate so large memory.";
    return nullptr;
  }
  if (LIKELY(consumption <= availableSize_)) {
    void* ptr = currentPtr_ + pad;
    currentPtr_ += consumption;
#ifndef NDEBUG
    allocatedSize_ += consumption;
#endif
    availableSize_ -= consumption;
    return ptr;
  } else {
    newChunk(std::max(alloc, kMinChunkSize));
    // The new operator will allocate the aligned memory
    DCHECK_EQ(reinterpret_cast<uintptr_t>(currentPtr_) & (kAlignment - 1), 0);
    void* ptr = currentPtr_;
    currentPtr_ += alloc;
#ifndef NDEBUG
    allocatedSize_ += alloc;
#endif
    availableSize_ -= alloc;
    return ptr;
  }
}

}  // namespace nebula
