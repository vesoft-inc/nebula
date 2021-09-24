/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexDedupNode.h"
namespace nebula {
namespace storage {
IndexDedupNode::IndexDedupNode(RuntimeContext* context, const std::vector<std::string>& dedupColumn)
    : IndexNode(context, "IndexDedupNode"), dedupColumns_(dedupColumn) {
  Map<std::string, size_t> m;
  for (size_t i = 0; i < inputColumn.size(); i++) {
    m[inputColumn[i]] = i;
  }
  for (auto& col : dedupColumn) {
    DCHECK(m.count(col));
    dedupPos_.push_back(m.at(col));
  }
}
}  // namespace storage
}  // namespace nebula
