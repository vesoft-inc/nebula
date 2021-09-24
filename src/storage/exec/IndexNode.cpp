/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/exec/IndexNode.h"

#include "folly/Likely.h"
namespace nebula {
namespace storage {
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
