/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_UTILS_TOPKHEAP_H_
#define COMMON_UTILS_TOPKHEAP_H_

#include "common/base/Base.h"

namespace nebula {

template <class T>
class TopKHeap final {
 public:
  TopKHeap(int heapSize, std::function<bool(T, T)> comparator)
      : heapSize_(heapSize), comparator_(std::move(comparator)) {
    v_.reserve(heapSize);
  }
  ~TopKHeap() = default;

  void push(T data) {
    if (v_.size() < static_cast<size_t>(heapSize_)) {
      v_.push_back(data);
      adjustUp(v_.size() - 1);
      return;
    }
    if (comparator_(data, v_[0])) {
      v_[0] = data;
      adjustDown(0);
    }
  }

  std::vector<T> moveTopK() { return std::move(v_); }

 private:
  void adjustDown(size_t parent) {
    size_t child = parent * 2 + 1;
    size_t size = v_.size();
    while (child < size) {
      if (child + 1 < size && comparator_(v_[child], v_[child + 1])) {
        child += 1;
      }
      if (!comparator_(v_[parent], v_[child])) {
        return;
      }
      std::swap(v_[parent], v_[child]);
      parent = child;
      child = parent * 2 + 1;
    }
  }
  void adjustUp(size_t child) {
    size_t parent = (child - 1) >> 1;
    while (0 != child) {
      if (!comparator_(v_[parent], v_[child])) {
        return;
      }
      std::swap(v_[parent], v_[child]);
      child = parent;
      parent = (child - 1) >> 1;
    }
  }

 private:
  int heapSize_;
  std::vector<T> v_;
  std::function<bool(T, T)> comparator_;
};
}  // namespace nebula
#endif  // COMMON_UTILS_TOPKHEAP_H_
