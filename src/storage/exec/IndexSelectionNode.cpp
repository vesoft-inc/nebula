/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/IndexSelectionNode.h"
namespace nebula {
namespace storage {
IndexSelectionNode::IndexSelectionNode(const IndexSelectionNode& node)
    : IndexNode(node), expr_(node.expr_), colPos_(node.colPos_) {
  ctx_ = std::make_unique<IndexExprContext>(colPos_);
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
  ctx_ = std::make_unique<IndexExprContext>(colPos_);
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

IndexNode::Result IndexSelectionNode::doNext() {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  do {
    auto result = child.next();
    if (!result.hasData()) {
      return result;
    }
    if (filter(result.row())) {
      return result;
    }
  } while (true);
  return Result();
}

std::unique_ptr<IndexNode> IndexSelectionNode::copy() {
  return std::make_unique<IndexSelectionNode>(*this);
}

std::string IndexSelectionNode::identify() {
  return fmt::format("{}(expr=[{}])", name_, expr_->toString());
}

}  // namespace storage

}  // namespace nebula
