/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/exec/IndexNode.h"

#include "folly/Likely.h"
namespace nebula {
namespace storage {
IndexNode::IndexNode(RuntimeContext* context, const std::string& name)
    : context_(context), name_(name) {
  spaceId_ = context_->spaceId();
}

IndexNode::IndexNode(const IndexNode& node)
    : context_(node.context_), spaceId_(node.spaceId_), name_(node.name_) {}

nebula::cpp2::ErrorCode IndexNode::doExecute(PartitionID partId) {
  for (auto& child : children_) {
    auto ret = child->execute(partId);
    if (UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
      return ret;
    }
  }
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
