/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexSelectionNode.h"
namespace nebula {
namespace storage {
IndexSelectionNode::IndexSelectionNode(const IndexSelectionNode& node)
    : IndexNode(node), expr_(node.expr_), colPos_(node.colPos_) {
  ctx_ = std::make_unique<ExprContext>(colPos_);
}

IndexSelectionNode::IndexSelectionNode(RuntimeContext* context, Expression* expr)
    : IndexNode(context, "IndexSelectionNode"), expr_(expr) {}
nebula::cpp2::ErrorCode IndexSelectionNode::init(InitContext& ctx) {
  DCHECK_EQ(children_.size(), 1);
  SelectionExprVisitor vis;
  expr_->accept(&vis);
  for (auto& col : vis.getRequiredColumns()) {
    ctx.requiredColumns.insert(col);
  }
  auto ret = children_[0]->init(ctx);
  if (UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  for (auto& col : vis.getRequiredColumns()) {
    colPos_[col] = ctx.retColMap.at(col);
  }
  ctx_ = std::make_unique<ExprContext>(colPos_);
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}
IndexNode::ErrorOr<Row> IndexSelectionNode::doNext(bool& hasNext) {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  do {
    auto result = child.next(hasNext);
    if (!hasNext || !nebula::ok(result)) {
      return result;
    }
    if (filter(nebula::value(result))) {
      return result;
    }
  } while (true);
  return ErrorOr<Row>(Row());
}
std::unique_ptr<IndexNode> IndexSelectionNode::copy() {
  return std::make_unique<IndexSelectionNode>(*this);
}
Value IndexSelectionNode::ExprContext::getEdgeProp(const std::string& edgeType,
                                                   const std::string& prop) const {
  UNUSED(edgeType);
  DCHECK(row_ != nullptr);
  auto iter = colPos_.find(prop);
  DCHECK(iter != colPos_.end());
  DCHECK(iter->second < row_->size());
  return row_[iter->second];
}
Value IndexSelectionNode::ExprContext::getTagProp(const std::string& tag,
                                                  const std::string& prop) const {
  UNUSED(tag);
  DCHECK(row_ != nullptr);
  auto iter = colPos_.find(prop);
  DCHECK(iter != colPos_.end());
  DCHECK(iter->second < row_->size());
  return row_[iter->second];
}

}  // namespace storage

}  // namespace nebula
