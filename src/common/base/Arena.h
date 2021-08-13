/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <cstddef>
#include <limits>
#include <type_traits>
#include <vector>

#include "common/base/Logging.h"
#include "common/cpp/helpers.h"

namespace nebula {

// MT-unsafe arena allocator
// It's optimized for many small objects construct/destruct
class Arena : public cpp::NonCopyable, cpp::NonMovable {
 public:
  ~Arena() {
    for (auto *ptr : chunks_) {
      delete[] ptr;
    }
    allocatedSize_ = 0;
    availableSize_ = 0;
    currentPtr_ = nullptr;
  }

  // The CPU access memory with the alignment,
  // So construct object from alignment address will reduce the CPU access count then
  // speed up read/write
  void *allocateAligned(const std::size_t alloc);

  std::size_t allocatedSize() const { return allocatedSize_; }

  std::size_t availableSize() const { return availableSize_; }

 private:
  static constexpr std::size_t kMinChunkSize = 4096;
  static constexpr std::size_t kMaxChunkSize = std::numeric_limits<uint16_t>::max();
  static constexpr std::size_t kAlignment = std::alignment_of<std::max_align_t>::value;

  // allocate new chunk
  void newChunk(std::size_t size) {
    DCHECK_NE(size, 0);
    // Expansion the vector first to avoid leak by allocate exception throw
    chunks_.emplace_back(nullptr);
    chunks_.back() = new uint8_t[size];
    availableSize_ = size;
    currentPtr_ = chunks_.back();
  }

  std::vector<uint8_t *> chunks_;
  // total size allocated
  std::size_t allocatedSize_{0};
  // total size which available to allocate
  std::size_t availableSize_{0};
  // The total chunks size
  // = allocatedSize_ + availableSize_ + Memory Deprecated (Size can't fit allocation)
  // Current pointer to available memory address
  uint8_t *currentPtr_{nullptr};
};

}  // namespace nebula
