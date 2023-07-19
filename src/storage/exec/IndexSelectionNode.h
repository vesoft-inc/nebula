/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXSELECTIONNODE_H
#define STORAGE_EXEC_INDEXSELECTIONNODE_H

#include "common/context/ExpressionContext.h"
#include "common/expression/ExprVisitorImpl.h"
#include "common/expression/Expression.h"
#include "folly/container/F14Map.h"
#include "storage/exec/IndexExprContext.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
/**
 *
 * IndexSelectionNode
 *
 * reference: IndexNode
 *
 * `IndexSelectionNode` is the class which is used to filter data by given expression in RPC
 * request.
 *                   ┌───────────┐
 *                   │ IndexNode │
 *                   └─────┬─────┘
 *                         │
 *               ┌─────────┴──────────┐
 *               │ IndexSelectionNode │
 *               └────────────────────┘
 * Member:
 * `expr_`  : expression used to filter
 * `colPos_`: column's position in Row which is during eval `expr_`
 * `ctx_`   : used to eval expression
 * Function:
 * `filter` : compute `expr_`
 *
 *
 * ------------------------------------------------------------------------------------------------
 * IndexSelectionNode::ExprContext
 *
 * `ExprContext` is a derive class of ExpressionContext which is needed in eval expression.
 *  NOTICE: There are many node in the entire storage plan tree where expressions need to be
 *          evaluated(e.g., Projection,Aggregate,etc.). So `ExprContext` may be not an internal
 *          class of IndexSelectionNode.
 */
class IndexSelectionNode : public IndexNode {
 public:
  IndexSelectionNode(const IndexSelectionNode &node);
  IndexSelectionNode(RuntimeContext *context, Expression *expr);
  nebula::cpp2::ErrorCode init(InitContext &ctx) override;
  std::unique_ptr<IndexNode> copy() override;
  std::string identify() override;

 private:
  Result doNext() override;
  inline bool filter(const Row &row) {
    ctx_->setRow(row);
    auto &result = expr_->eval(*ctx_);
    return result.type() == Value::Type::BOOL ? result.getBool() : false;
  }
  Expression *expr_;
  Map<std::string, size_t> colPos_;
  // TODO(hs.zhang): `ExprContext` could be moved out later if we unify the volcano in go/lookup
  std::unique_ptr<IndexExprContext> ctx_;
};

class SelectionExprVisitor : public ExprVisitorImpl {
 public:
  void visit(EdgeSrcIdExpression *expr) override {
    requiredColumns_.insert(expr->prop());
  }
  void visit(EdgeTypeExpression *expr) override {
    requiredColumns_.insert(expr->prop());
  }
  void visit(EdgeRankExpression *expr) override {
    requiredColumns_.insert(expr->prop());
  }
  void visit(EdgeDstIdExpression *expr) override {
    requiredColumns_.insert(expr->prop());
  }
  void visit(TagPropertyExpression *expr) override {
    requiredColumns_.insert(expr->prop());
  }
  void visit(EdgePropertyExpression *expr) override {
    requiredColumns_.insert(expr->prop());
  }
  void visit(LabelTagPropertyExpression *expr) override {
    requiredColumns_.insert(expr->prop());
  }
  const Set<std::string> &getRequiredColumns() {
    return requiredColumns_;
  }
  ::nebula::cpp2::ErrorCode getCode() {
    return code_;
  }

 private:
  using ExprVisitorImpl::visit;
  Set<std::string> requiredColumns_;
  ::nebula::cpp2::ErrorCode code_;
};

}  // namespace storage

}  // namespace nebula
#endif
