/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexLimitNode.h"
namespace nebula {
namespace storage {
IndexLimitNode::IndexLimitNode(RuntimeContext* context, uint64_t offset, uint64_t limit)
    : IndexNode(context, "IndexLimitNode"), offset_(offset), limit_(limit) {}
IndexLimitNode::IndexLimitNode(RuntimeContext* context, uint64_t limit)
    : IndexLimitNode(context, 0, limit) {}
nebula::cpp2::ErrorCode IndexLimitNode::doExecute(PartitionID partId) override {
  currentOffset_ = 0;
  return children_[0]->execute(partId);
}
IndexNode::ErrorOr<Row> IndexLimitNode::doNext(bool& hasNext) override {
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
}  // namespace storage
}  // namespace nebula
