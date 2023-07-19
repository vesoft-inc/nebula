/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/IndexDedupNode.h"
namespace nebula {
namespace storage {
IndexDedupNode::IndexDedupNode(const IndexDedupNode& node)
    : IndexNode(node), dedupColumns_(node.dedupColumns_), dedupPos_(node.dedupPos_) {}

IndexDedupNode::IndexDedupNode(RuntimeContext* context, const std::vector<std::string>& dedupColumn)
    : IndexNode(context, "IndexDedupNode"), dedupColumns_(dedupColumn) {}
::nebula::cpp2::ErrorCode IndexDedupNode::init(InitContext& ctx) {
  for (auto& col : dedupColumns_) {
    ctx.requiredColumns.insert(col);
  }
  // The contents of `ctx` should be the same when all child nodes are initialized, and `ctx` should
  // be the same after initialization.
  for (size_t i = 0; i < children_.size() - 1; i++) {
    auto tmp = ctx;  //
    auto ret = children_[i]->init(tmp);
    if (ret != ::nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
  }
  auto ret = children_.back()->init(ctx);
  if (ret != ::nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }
  for (auto& col : dedupColumns_) {
    dedupPos_.push_back(ctx.retColMap[col]);
  }
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

::nebula::cpp2::ErrorCode IndexDedupNode::doExecute(PartitionID partId) {
  currentChild_ = 0;
  dedupSet_.clear();
  return IndexNode::doExecute(partId);
}

IndexNode::Result IndexDedupNode::iterateCurrentChild(size_t currentChild) {
  auto& child = *children_[currentChild];
  Result result;
  do {
    result = child.next();
    // error or meet end
    if (!result.hasData()) {
      return result;
    }
    auto dedupResult = dedup(result.row());
    if (!dedupResult) {
      continue;
    }
    return result;
  } while (true);
}

IndexNode::Result IndexDedupNode::doNext() {
  Result result;
  while (currentChild_ < children_.size()) {
    result = iterateCurrentChild(currentChild_);
    // error
    if (!result.success()) {
      return result;
    }
    // finish iterate one child
    if (!result.hasData()) {
      currentChild_++;
      continue;
    }
    return result;
  }
  return Result();
}

IndexDedupNode::RowWrapper::RowWrapper(const Row& row, const std::vector<size_t>& posList) {
  values_.reserve(posList.size());
  for (auto p : posList) {
    values_.emplace_back(row[p]);
  }
}

std::unique_ptr<IndexNode> IndexDedupNode::copy() {
  return std::make_unique<IndexDedupNode>(*this);
}

std::string IndexDedupNode::identify() {
  return fmt::format("{}(dedup=[{}])", name_, folly::join(',', dedupColumns_));
}

}  // namespace storage
}  // namespace nebula
