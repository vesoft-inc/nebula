// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include <folly/Likely.h>

#include <boost/core/noncopyable.hpp>
#include <cstddef>
#include <limits>
#include <type_traits>

#include "common/base/Logging.h"
#include "common/cpp/helpers.h"

namespace nebula {

// MT-unsafe arena allocator
// It's optimized for many small objects construct/destruct
class Arena : public boost::noncopyable, cpp::NonMovable {
 public:
  ~Arena() {
    while (LIKELY(currentChunk_ != nullptr)) {
      auto *prev = currentChunk_->prev;
      delete[] currentChunk_;
      currentChunk_ = prev;
    }
#ifndef NDEBUG
    allocatedSize_ = 0;
#endif
    availableSize_ = 0;
    currentPtr_ = nullptr;
  }

  // The CPU access memory with the alignment,
  // So construct object from alignment address will reduce the CPU access count then
  // speed up read/write
  void *allocateAligned(const std::size_t alloc);

#ifndef NDEBUG
  std::size_t allocatedSize() const {
    return allocatedSize_;
  }
#endif

  std::size_t availableSize() const {
    return availableSize_;
  }

 private:
  static constexpr std::size_t kMinChunkSize = 4096;
  static constexpr std::size_t kMaxChunkSize = std::numeric_limits<uint16_t>::max();
  static constexpr std::size_t kAlignment = std::alignment_of<std::max_align_t>::value;

  struct Chunk {
    explicit Chunk(Chunk *p) : prev{p} {}

    union {
      Chunk *prev{nullptr};
      std::byte aligned[kAlignment];
    };
  };

  // allocate new chunk
  // The current pointer will keep alignment
  void newChunk(std::size_t size) {
    DCHECK_NE(size, 0);
    std::byte *ptr = new std::byte[size + sizeof(Chunk)];
    currentChunk_ = new (ptr) Chunk(currentChunk_);
    availableSize_ = size;
    currentPtr_ = (ptr + sizeof(Chunk));
  }

  Chunk *currentChunk_{nullptr};
// These are debug info
// Remove to speed up in Release build
#ifndef NDEBUG
  // total size allocated
  std::size_t allocatedSize_{0};
#endif
  // total size which available to allocate
  std::size_t availableSize_{0};
  // The total chunks size
  // = allocatedSize_ + availableSize_ + Memory Deprecated (Size can't fit allocation)
  // Current pointer to available memory address
  std::byte *currentPtr_{nullptr};
};

}  // namespace nebula
