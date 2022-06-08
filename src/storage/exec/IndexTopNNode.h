/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXTOPNNODE_H
#define STORAGE_EXEC_INDEXTOPNNODE_H
#include "folly/Likely.h"
#include "storage/exec/IndexLimitNode.h"
namespace nebula {
namespace storage {
template <class T>
class TopNHeap final {
 public:
  ~TopNHeap() = default;

  void setHeapSize(uint64_t heapSize) {
    heapSize_ = heapSize;
    v_.reserve(heapSize);
  }

  void setComparator(std::function<bool(T&, T&)> comparator) {
    comparator_ = comparator;
  }

  void push(T&& data) {
    if (v_.size() < heapSize_) {
      v_.push_back(std::move(data));
      adjustUp(v_.size() - 1);
      return;
    }
    if (comparator_(data, v_[0])) {
      v_[0] = std::move(data);
      adjustDown(0);
    }
  }

  std::vector<T> moveTopK() {
    return std::move(v_);
  }

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
  std::function<bool(T&, T&)> comparator_;
  uint64_t heapSize_;
  std::vector<T> v_;
};

class IndexTopNNode : public IndexLimitNode {
 public:
  IndexTopNNode(const IndexTopNNode& node);
  IndexTopNNode(RuntimeContext* context,
                uint64_t offset,
                uint64_t limit,
                const std::vector<cpp2::OrderBy>* orderBy);
  IndexTopNNode(RuntimeContext* context, uint64_t limit, const std::vector<cpp2::OrderBy>* orderBy);
  nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;
  std::string identify() override;

 private:
  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override;
  Result doNext() override;
  void topN();
  const std::vector<cpp2::OrderBy> orderBy_;
  std::deque<Result> results_;
  bool finished_{false};
  std::vector<std::string> requiredColumns_;
  Map<std::string, size_t> colPos_;
};
}  // namespace storage

}  // namespace nebula
#endif
