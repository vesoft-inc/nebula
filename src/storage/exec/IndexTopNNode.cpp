/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/IndexTopNNode.h"
namespace nebula {
namespace storage {
IndexTopNNode::IndexTopNNode(const IndexTopNNode& node)
    : IndexLimitNode(node), orderBy_(node.orderBy_) {
  name_ = "IndexTopNNode";
}

IndexTopNNode::IndexTopNNode(RuntimeContext* context,
                             uint64_t offset,
                             uint64_t limit,
                             const std::vector<cpp2::OrderBy>* orderBy)
    : IndexLimitNode(context, offset, limit), orderBy_(orderBy) {
  name_ = "IndexTopNNode";
}
IndexTopNNode::IndexTopNNode(RuntimeContext* context,
                             uint64_t limit,
                             const std::vector<cpp2::OrderBy>* orderBy)
    : IndexTopNNode(context, 0, limit, orderBy) {}
nebula::cpp2::ErrorCode IndexTopNNode::init(InitContext& ctx) {
  DCHECK_EQ(children_.size(), 1);
  for (auto iter = orderBy_->begin(); iter != orderBy_->end(); iter++) {
    ctx.requiredColumns.insert(iter->get_prop());
  }
  auto ret = children_[0]->init(ctx);
  if (UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  for (auto iter = orderBy_->begin(); iter != orderBy_->end(); iter++) {
    auto pos = ctx.retColMap.find(iter->get_prop());
    DCHECK(pos != ctx.retColMap.end());
    colPos_[iter->get_prop()] = pos->second;
  }
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}
nebula::cpp2::ErrorCode IndexTopNNode::doExecute(PartitionID partId) {
  finished_ = false;
  return children_[0]->execute(partId);
}

IndexNode::Result IndexTopNNode::doNext() {
  DCHECK_EQ(children_.size(), 1);
  if (!finished_) {
    topN();
  }
  if (results_.empty()) {
    return Result();
  }
  auto result = results_.front();
  results_.pop_front();
  return result;
}

void IndexTopNNode::topN() {
  if (offset_ + limit_ <= 0) {
    results_.emplace_back(Result());
    finished_ = true;
    return;
  }
  auto comparator = [this](Row& lhs, Row& rhs) -> bool {
    for (auto iter = orderBy_->begin(); iter != orderBy_->end(); iter++) {
      auto prop = iter->get_prop();
      auto orderType = iter->get_direction();
      auto lValue = lhs[colPos_[prop]];
      auto rValue = rhs[colPos_[prop]];
      if (lValue == rValue) {
        continue;
      }
      if (orderType == cpp2::OrderDirection::ASCENDING) {
        return lValue < rValue;
      } else if (orderType == cpp2::OrderDirection::DESCENDING) {
        return lValue > rValue;
      }
    }
    return false;
  };

  TopNHeap<Row> topNHeap;
  topNHeap.setHeapSize(offset_ + limit_);
  topNHeap.setComparator(comparator);

  auto& child = *children_[0];
  do {
    auto result = child.next();
    if (!result.success()) {
      results_.emplace_back(result);
      finished_ = true;
      return;
    }
    if (result.hasData()) {
      topNHeap.push(std::move(result).row());
    } else {
      break;
    }
  } while (true);
  auto topNData = topNHeap.moveTopK();
  for (auto iter = topNData.begin(); iter != topNData.end(); iter++) {
    results_.emplace_back(std::move(*iter));
  }
  finished_ = true;
}

std::unique_ptr<IndexNode> IndexTopNNode::copy() {
  return std::make_unique<IndexTopNNode>(*this);
}

std::string IndexTopNNode::identify() {
  std::stringstream ss;
  for (auto iter = orderBy_->begin(); iter != orderBy_->end(); iter++) {
    auto orderType = iter->get_direction();
    if (orderType == cpp2::OrderDirection::ASCENDING) {
      ss << iter->get_prop() << " asc,";
    } else {
      ss << iter->get_prop() << " desc,";
    }
  }
  if (offset_ > 0) {
    return fmt::format("{}({} offset={}, limit={})", name_, ss.str(), offset_, limit_);
  } else {
    return fmt::format("{}({} limit={})", name_, ss.str(), limit_);
  }
}

}  // namespace storage
}  // namespace nebula
