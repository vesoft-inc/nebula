/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once

#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
class IndexProjectionNode : public IndexNode {
 public:
  IndexProjectionNode(RuntimeContext* context,
                      const std::vector<std::string>& requiredColumns,
                      const std::vector<std::string>& inputColumns);
  nebula::cpp2::ErrorCode execute(PartitionID partId) = 0;
  ErrorOr<Row> next(bool& hasNext) override {
    DCHECK_EQ(children_.size(), 1);
    auto& child = *children_[0];
    auto result = child.next(hasNext);
    if (::nebula::ok(result) && hasNext) {
      return ErrorOr<Row>(project(::nebula::value(std::move(result))));
    } else {
      return ErrorOr<Row>(Row());
    }
  }

 private:
  Row project(Row&& row) {
    Row ret;
    for (auto& col : requiredColumns_) {
      ret.emplace_back(std::move(row[colPos_[col]]));
    }
  }
  std::vector<std::string> requiredColumns_;
  Map<std::string, size_t> colPos_;
};
}  // namespace storage
}  // namespace nebula
