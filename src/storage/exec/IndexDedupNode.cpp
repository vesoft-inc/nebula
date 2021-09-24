/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexDedupNode.h"
namespace nebula {
namespace storage {
IndexDedupNode::IndexDedupNode(RuntimeContext* context, const std::vector<std::string>& dedupColumn)
    : IndexNode(context, "IndexDedupNode"), dedupColumns_(dedupColumn) {}
::nebula::cpp2::ErrorCode IndexDedupNode::init(InitContext& ctx) {
  for (auto& col : dedupColumns_) {
    ctx.requiredColumns.insert(col);
  }
  // The return Row format of each child node must be the same
  InitContext childCtx = ctx;
  for (auto& child : children_) {
    child->init(childCtx);
    childCtx = ctx;
  }
  ctx = childCtx;
  for (auto& col : dedupColumns_) {
    dedupPos_.push_back(ctx.retColMap[col]);
  }
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}
::nebula::cpp2::ErrorCode IndexDedupNode::doExecute(PartitionID partId) {
  dedupSet_.clear();
  return IndexNode::doExecute(partId);
}
IndexNode::ErrorOr<Row> IndexDedupNode::doNext(bool& hasNext) override {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  do {
    auto result = child.next(hasNext);
    if (!hasNext || !nebula::ok(result)) {
      return result;
    }
    if (dedup(::nebula::value(result))) {
      return result;
    }
  } while (true);
}
IndexDedupNode::RowWrapper::RowWrapper(const Row& row, const std::vector<size_t>& posList) {
  for (auto p : posList) {
    values_.emplace_back(row[p]);
  }
}
}  // namespace storage
}  // namespace nebula
