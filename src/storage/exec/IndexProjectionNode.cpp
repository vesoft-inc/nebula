/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/IndexProjectionNode.h"
namespace nebula {
namespace storage {
IndexProjectionNode::IndexProjectionNode(const IndexProjectionNode& node)
    : IndexNode(node), requiredColumns_(node.requiredColumns_), colPos_(node.colPos_) {}
IndexProjectionNode::IndexProjectionNode(RuntimeContext* context,
                                         const std::vector<std::string>& requiredColumns)
    : IndexNode(context, "IndexProjectionNode"), requiredColumns_(requiredColumns) {}
nebula::cpp2::ErrorCode IndexProjectionNode::init(InitContext& ctx) {
  DCHECK_EQ(children_.size(), 1);
  for (auto& col : requiredColumns_) {
    ctx.requiredColumns.insert(col);
  }
  for (auto& col : ctx.statColumns) {
    if (ctx.requiredColumns.find(col) == ctx.requiredColumns.end()) {
      ctx.requiredColumns.insert(col);
      requiredColumns_.push_back(col);
    }
  }
  auto ret = children_[0]->init(ctx);
  if (UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  for (auto& col : requiredColumns_) {
    auto iter = ctx.retColMap.find(col);
    DCHECK(iter != ctx.retColMap.end());
    colPos_[col] = iter->second;
  }
  ctx.returnColumns = requiredColumns_;
  ctx.retColMap.clear();
  for (size_t i = 0; i < ctx.returnColumns.size(); i++) {
    ctx.retColMap[ctx.returnColumns[i]] = i;
  }
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

IndexNode::Result IndexProjectionNode::doNext() {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  Result result = child.next();
  if (result.hasData()) {
    result = Result(project(std::move(result).row()));
  }
  return result;
}

Row IndexProjectionNode::project(Row&& row) {
  Row ret;
  ret.reserve(requiredColumns_.size());
  for (auto& col : requiredColumns_) {
    ret.emplace_back(std::move(row[colPos_[col]]));
  }
  return ret;
}

std::unique_ptr<IndexNode> IndexProjectionNode::copy() {
  return std::make_unique<IndexProjectionNode>(*this);
}

std::string IndexProjectionNode::identify() {
  return fmt::format("{}(projectColumn=[{}])", name_, folly::join(",", requiredColumns_));
}

}  // namespace storage
}  // namespace nebula
