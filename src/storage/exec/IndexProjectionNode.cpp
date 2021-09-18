/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexProjectionNode.h"
namespace nebula {
namespace storage {
IndexProjectionNode::IndexProjectionNode(RuntimeContext* context,
                                         const std::vector<std::string>& requiredColumns,
                                         const std::vector<std::string>& inputColumns)
    : IndexNode(context), requiredColumns_(requiredColumns) {
  for (size_t i = 0; i < inputColumns.size(); i++) {
    colPos_[inputColumns[i]] = i;
  }
}
IndexNode::ErrorOr<Row> IndexProjectionNode::next(bool& hasNext) {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  auto result = child.next(hasNext);
  if (::nebula::ok(result) && hasNext) {
    return ErrorOr<Row>(project(::nebula::value(std::move(result))));
  } else {
    return ErrorOr<Row>(Row());
  }
}
Row IndexProjectionNode::project(Row&& row) {
  Row ret;
  for (auto& col : requiredColumns_) {
    ret.emplace_back(std::move(row[colPos_[col]]));
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
