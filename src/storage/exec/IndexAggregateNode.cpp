/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/exec/IndexAggregateNode.h"

namespace nebula {
namespace storage {

IndexAggregateNode::IndexAggregateNode(const IndexAggregateNode& node)
    : IndexNode(node), statInfos_(node.statInfos_), returnColumnsCount_(node.returnColumnsCount_) {
  stats_ = node.stats_;
  retColMap_ = node.retColMap_;
}

IndexAggregateNode::IndexAggregateNode(
    RuntimeContext* context,
    const std::vector<std::pair<std::string, cpp2::StatType>>& statInfos,
    size_t returnColumnsCount)
    : IndexNode(context, "IndexAggregateNode"),
      statInfos_(statInfos),
      returnColumnsCount_(returnColumnsCount) {}

nebula::cpp2::ErrorCode IndexAggregateNode::init(InitContext& ctx) {
  DCHECK_EQ(children_.size(), 1);
  for (const auto& statInfo : statInfos_) {
    ctx.statColumns.insert(statInfo.first);
  }
  auto ret = children_[0]->init(ctx);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }
  initStatValue();
  retColMap_.clear();
  retColMap_ = ctx.retColMap;
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void IndexAggregateNode::initStatValue() {
  stats_.clear();
  if (statInfos_.size() > 0) {
    stats_.reserve(statInfos_.size());
    for (const auto& statInfo : statInfos_) {
      stats_.emplace_back(statInfo.second);
    }
  }
}

void IndexAggregateNode::addStatValue(const Value& value, ColumnStat* stat) {
  switch (stat->statType_) {
    case cpp2::StatType::SUM: {
      stat->sum_ = stat->sum_ + value;
      break;
    }
    case cpp2::StatType::COUNT: {
      stat->count_ = stat->count_ + 1;
      break;
    }
    case cpp2::StatType::MAX: {
      stat->max_ = value > stat->max_ ? value : stat->max_;
      break;
    }
    case cpp2::StatType::MIN: {
      stat->min_ = value < stat->min_ ? value : stat->min_;
      break;
    }
    default:
      LOG(ERROR) << "get invalid stat type";
      return;
  }
}

Row IndexAggregateNode::project(Row&& row) {
  Row ret;
  ret.reserve(returnColumnsCount_);
  for (size_t i = 0; i < returnColumnsCount_; i++) {
    ret.emplace_back(std::move(row[i]));
  }
  return ret;
}

Row IndexAggregateNode::calculateStats() {
  Row result;
  result.values.reserve(stats_.size());
  for (const auto& stat : stats_) {
    if (stat.statType_ == cpp2::StatType::SUM) {
      result.values.emplace_back(stat.sum_);
    } else if (stat.statType_ == cpp2::StatType::COUNT) {
      result.values.emplace_back(stat.count_);
    } else if (stat.statType_ == cpp2::StatType::MAX) {
      result.values.emplace_back(stat.max_);
    } else if (stat.statType_ == cpp2::StatType::MIN) {
      result.values.emplace_back(stat.min_);
    }
  }
  return result;
}

IndexNode::Result IndexAggregateNode::doNext() {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  Result result = child.next();
  const auto& row = result.row();
  if (result.hasData()) {
    for (size_t i = 0; i < statInfos_.size(); i++) {
      const auto& columnName = statInfos_[i].first;
      addStatValue(row[retColMap_[columnName]], &stats_[i]);
    }
    result = Result(project(std::move(result).row()));
  }
  return result;
}

std::unique_ptr<IndexNode> IndexAggregateNode::copy() {
  return std::make_unique<IndexAggregateNode>(*this);
}

std::string IndexAggregateNode::identify() {
  return "";
}

}  // namespace storage

}  // namespace nebula
