/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/IndexLimitNode.h"
namespace nebula {
namespace storage {
IndexLimitNode::IndexLimitNode(const IndexLimitNode& node)
    : IndexNode(node), offset_(node.offset_), limit_(node.limit_) {}

IndexLimitNode::IndexLimitNode(RuntimeContext* context, uint64_t offset, uint64_t limit)
    : IndexNode(context, "IndexLimitNode"), offset_(offset), limit_(limit) {}
IndexLimitNode::IndexLimitNode(RuntimeContext* context, uint64_t limit)
    : IndexLimitNode(context, 0, limit) {}
nebula::cpp2::ErrorCode IndexLimitNode::doExecute(PartitionID partId) {
  currentOffset_ = 0;
  return children_[0]->execute(partId);
}

IndexNode::Result IndexLimitNode::doNext() {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  while (UNLIKELY(currentOffset_ < offset_)) {
    auto result = child.next();
    if (!result.hasData()) {
      return result;
    }
    currentOffset_++;
  }
  if (currentOffset_ < offset_ + limit_) {
    currentOffset_++;
    return child.next();
  } else {
    return Result();
  }
}

std::unique_ptr<IndexNode> IndexLimitNode::copy() {
  return std::make_unique<IndexLimitNode>(*this);
}

std::string IndexLimitNode::identify() {
  if (offset_ > 0) {
    return fmt::format("{}(offset={}, limit={})", name_, offset_, limit_);
  } else {
    return fmt::format("{}(limit={})", name_, limit_);
  }
}

}  // namespace storage
}  // namespace nebula
