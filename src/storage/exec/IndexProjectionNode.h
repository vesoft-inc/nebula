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
class IndexProjectionNode : public IndexNode {
 public:
  IndexProjectionNode(const IndexProjectionNode& node);
  IndexProjectionNode(RuntimeContext* context, const std::vector<std::string>& requiredColumns);
  nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;

 private:
  ErrorOr<Row> doNext(bool& hasNext) override;
  Row project(Row&& row);
  std::vector<std::string> requiredColumns_;
  Map<std::string, size_t> colPos_;
};
}  // namespace storage
}  // namespace nebula
