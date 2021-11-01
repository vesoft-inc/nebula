/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
  // The return Row format of each child node must be the same
  InitContext childCtx = ctx;
  InitContext ctx2;
  for (auto& child : children_) {
    auto ret = child->init(childCtx);
    if (ret != ::nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
    ctx2 = childCtx;
    childCtx = ctx;
  }
  ctx = ctx2;
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
IndexNode::ErrorOr<Row> IndexDedupNode::doNext(bool& hasNext) {
  Row ret;
  hasNext = false;
  while (currentChild_ < children_.size()) {
    auto& child = *children_[currentChild_];
    DVLOG(3) << currentChild_;
    do {
      auto result = child.next(hasNext);
      if (!nebula::ok(result)) {
        return result;
      }
      if (!hasNext) {
        break;
      }
      auto dedupResult = dedup(::nebula::value(result));
      DVLOG(3) << dedupResult << "\t" << ::nebula::value(result);
      if (dedupResult) {
        ret = ::nebula::value(std::move(result));
        hasNext = true;
        break;
      }
    } while (true);
    if (!hasNext) {
      currentChild_++;
    } else {
      break;
    }
  }
  return ret;
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
