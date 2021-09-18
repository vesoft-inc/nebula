/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "folly/Likely.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
class IndexLimitNode : public IndexNode {
 public:
  IndexLimitNode(RuntimeContext* context, uint64_t offset, uint64_t limit)
      : IndexNode(context), offset_(offset), limit_(limit) {}
  IndexLimitNode(RuntimeContext* context, uint64_t limit) : IndexLimitNode(context, 0, limit) {}
  nebula::cpp2::ErrorCode execute(PartitionID partId) override;
  ErrorOr<Row> next(bool& hasNext) override {
    DCHECK_EQ(children_.size(), 1);
    auto& child = *children_[0];
    while (UNLIKELY(currentOffset_ < offset_)) {
      auto result = child.next(hasNext);
      if (!::nebula::ok(result) || !hasNext) {
        return result;
      }
      currentOffset_++;
    }
    if (currentOffset_ < offset_ + limit_) {
      return child.next(hasNext);
    } else {
      hasNext = false;
      return ErrorOr<Row>(Row());
    }
  }

 private:
  const uint64_t offset_, limit_;
  uint64_t currentOffset_ = 0;
};
}  // namespace storage

}  // namespace nebula
