/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "ExtractFilterExprVisitor.h"

#include "graph/util/ExpressionUtils.h"

namespace nebula {
namespace graph {

void ExtractFilterExprVisitor::visit(ConstantExpression *) {
  canBePushed_ = true;
}

void ExtractFilterExprVisitor::visit(LabelExpression *) {
  canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(UUIDExpression *) {
  canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(VariableExpression *expr) {
  canBePushed_ = static_cast<VariableExpression *>(expr)->isInner();
}

void ExtractFilterExprVisitor::visit(VersionedVariableExpression *) {
  canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(TagPropertyExpression *expr) {
  if (expr->sym() == "*") {  // Storage don't support '*' for tag
    canBePushed_ = false;
    return;
  }
  switch (pushType_) {
    case PushType::kGetNeighbors:
      canBePushed_ = false;
      break;
    case PushType::kGetVertices:
      canBePushed_ = true;
      break;
    case PushType::kGetEdges:
      canBePushed_ = false;
      break;
  }
}

void ExtractFilterExprVisitor::visit(EdgePropertyExpression *) {
  switch (pushType_) {
    case PushType::kGetNeighbors:
    case PushType::kGetEdges:
      canBePushed_ = true;
      break;
    case PushType::kGetVertices:
      canBePushed_ = false;
      break;
  }
}

void ExtractFilterExprVisitor::visit(InputPropertyExpression *) {
  canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(LabelTagPropertyExpression *) {
  // Storage don't support this expression
  // Convert it to tag/vertex related property expression for before push-down
  canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(VariablePropertyExpression *expr) {
  auto colName = expr->prop();
  auto iter = std::find(colNames_.begin(), colNames_.end(), colName);
  canBePushed_ = iter != colNames_.end();
}

void ExtractFilterExprVisitor::visit(DestPropertyExpression *) {
  switch (pushType_) {
    case PushType::kGetNeighbors:
    case PushType::kGetEdges:
      canBePushed_ = false;
      break;
    case PushType::kGetVertices:
      canBePushed_ = true;
      break;
  }
}

void ExtractFilterExprVisitor::visit(SourcePropertyExpression *) {
  switch (pushType_) {
    case PushType::kGetNeighbors:
      canBePushed_ = true;
      break;
    case PushType::kGetVertices:
    case PushType::kGetEdges:
      canBePushed_ = false;
      break;
  }
}

void ExtractFilterExprVisitor::visit(EdgeSrcIdExpression *) {
  switch (pushType_) {
    case PushType::kGetNeighbors:
    case PushType::kGetEdges:
      canBePushed_ = true;
      break;
    case PushType::kGetVertices:
      canBePushed_ = false;
      break;
  }
}

void ExtractFilterExprVisitor::visit(EdgeTypeExpression *) {
  switch (pushType_) {
    case PushType::kGetNeighbors:
    case PushType::kGetEdges:
      canBePushed_ = true;
      break;
    case PushType::kGetVertices:
      canBePushed_ = false;
      break;
  }
}

void ExtractFilterExprVisitor::visit(EdgeRankExpression *) {
  switch (pushType_) {
    case PushType::kGetNeighbors:
    case PushType::kGetEdges:
      canBePushed_ = true;
      break;
    case PushType::kGetVertices:
      canBePushed_ = false;
      break;
  }
}

void ExtractFilterExprVisitor::visit(EdgeDstIdExpression *) {
  switch (pushType_) {
    case PushType::kGetNeighbors:
    case PushType::kGetEdges:
      canBePushed_ = true;
      break;
    case PushType::kGetVertices:
      canBePushed_ = false;
      break;
  }
}

void ExtractFilterExprVisitor::visit(VertexExpression *) {
  canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(EdgeExpression *) {
  canBePushed_ = false;
}

void ExtractFilterExprVisitor::visit(ColumnExpression *) {
  canBePushed_ = false;
}

// @return: whether this logical expr satisfies split condition
bool ExtractFilterExprVisitor::visitLogicalAnd(LogicalExpression *expr, std::vector<bool> &flags) {
  DCHECK_EQ(expr->kind(), Expression::Kind::kLogicalAnd);
  // if the size of operands greater than 2
  // then we think it has been pullAnds before
  if (expr->operands().size() <= 2) {
    ExpressionUtils::pullAnds(expr);
  }
  auto &operands = expr->operands();
  auto canBePushed = false;
  bool allOpCanBePushed = true;
  auto size = operands.size();
  flags.resize(operands.size(), false);
  for (auto i = 0u; i < size; i++) {
    canBePushed_ = true;
    if (operands[i]->kind() == Expression::Kind::kLogicalOr) {
      auto lgExpr = static_cast<LogicalExpression *>(operands[i]);
      bool isSplit = visitLogicalOr(lgExpr);
      if (isSplit) {
        // if split, the size of subOperands must be 2,
        // subOperands[0] must can be pushed expr
        // subOperands[1] must can not push expr
        auto &subOperands = lgExpr->operands();
        DCHECK_EQ(subOperands.size(), 2);
        expr->setOperand(i, subOperands[0]->clone());
        expr->addOperand(subOperands[1]->clone());
        flags.emplace_back(false);
        allOpCanBePushed = false;
      }
    } else {
      operands[i]->accept(this);
    }
    flags[i] = canBePushed_;
    allOpCanBePushed = allOpCanBePushed && canBePushed_;
    canBePushed = canBePushed || canBePushed_;
  }
  canBePushed_ = canBePushed;
  if (!canBePushed_) {
    // all operands can not be pushed
    return false;
  }
  if (isNested_) {
    // if canBePushed is true and allOpCanBePushed is false
    // it means some of the operands can not be pushed and can be split
    canBePushed_ = allOpCanBePushed;
    return !allOpCanBePushed;
  }
  if (allOpCanBePushed) {
    return false;
  }
  ExtractRemainExpr(expr, flags);
  return false;
}

void ExtractFilterExprVisitor::ExtractRemainExpr(LogicalExpression *expr,
                                                 std::vector<bool> &flags) {
  DCHECK_EQ(expr->kind(), Expression::Kind::kLogicalAnd);
  auto &operands = expr->operands();
  std::vector<Expression *> remainedOperands;
  auto lastUsedExprInd = 0u;
  for (auto i = 0u; i < operands.size(); i++) {
    if (!flags[i]) {
      remainedOperands.emplace_back(operands[i]->clone());
    } else {
      if (lastUsedExprInd < i) {
        expr->setOperand(lastUsedExprInd, operands[i]);
      }
      lastUsedExprInd++;
    }
  }

  operands.resize(lastUsedExprInd);
  if (lastUsedExprInd > 1) {
    auto extractedExpr = LogicalExpression::makeAnd(pool_);
    extractedExpr->setOperands(operands);
    extractedExpr_ = std::move(extractedExpr);
  } else {
    extractedExpr_ = operands[0];
  }

  LogicalExpression *remainedExpr = nullptr;
  if (remainedOperands.size() > 1) {
    remainedExpr = LogicalExpression::makeAnd(pool_);
    remainedExpr->setOperands(std::move(remainedOperands));
  } else {
    remainedExpr = static_cast<LogicalExpression *>(std::move(remainedOperands[0]));
  }
  if (remainedExpr_ != nullptr) {
    if (remainedExprFromAnd_) {
      auto mergeExpr = LogicalExpression::makeAnd(pool_, remainedExpr_, std::move(remainedExpr));
      remainedExpr_ = std::move(mergeExpr);
      remainedExprFromAnd_ = true;
    } else {
      auto mergeExpr = LogicalExpression::makeOr(pool_, remainedExpr_, std::move(remainedExpr));
      remainedExpr_ = std::move(mergeExpr);
      remainedExprFromAnd_ = true;
    }
  } else {
    remainedExpr_ = std::move(remainedExpr);
    remainedExprFromAnd_ = true;
  }
}

// @return: split or not
bool ExtractFilterExprVisitor::visitLogicalOr(LogicalExpression *expr) {
  DCHECK_EQ(expr->kind(), Expression::Kind::kLogicalOr);
  if (expr->operands().size() <= 2) {
    ExpressionUtils::pullOrs(expr);
  }
  // For simplification,
  // LogicalOr can be split,
  // if and only if just one operand can not be pushed and that operand is LogicalAnd.
  // splited means:
  //      canBePush1 or (canBePush2 and ... and canNotBePush3)
  //      => (canBePush1 or canBePush2) and ... and (canBePush1 or canNotBePush3)
  auto &operands = expr->operands();
  int canNotPushedIndex = -1;
  std::vector<bool> flags;
  canBePushed_ = true;
  for (auto i = 0u; i < operands.size(); i++) {
    if (operands[i]->kind() == Expression::Kind::kLogicalAnd) {
      auto lgExpr = static_cast<LogicalExpression *>(operands[i]);
      std::vector<bool> flag;
      bool isNested = isNested_;
      isNested_ = true;
      bool canBeSplit = visitLogicalAnd(lgExpr, flag);
      isNested_ = isNested;
      if (!canBePushed_) {
        // if the LogicalAnd can not be split, return
        if (canNotPushedIndex != -1 || !canBeSplit) {
          return false;
        }
        canBePushed_ = true;  // reset when the LogicalAnd can be split
        canNotPushedIndex = i;
        flags = std::move(flag);
      }
    } else {
      operands[i]->accept(this);
      if (!canBePushed_) {
        return false;
      }
    }
  }

  if (canNotPushedIndex == -1 || splitForbidden) {
    canBePushed_ = (canNotPushedIndex == -1);
    return false;
  }
  // only one operand, which is LogicalAnd expr, can not be pushed. Split it.
  splitOrExpr(expr, flags, canNotPushedIndex);
  return canBePushed_;
}

void ExtractFilterExprVisitor::splitOrExpr(LogicalExpression *expr,
                                           std::vector<bool> &flags,
                                           const unsigned int canNotPushedIndex) {
  auto &operands = expr->operands();
  auto andExpr = operands[canNotPushedIndex];
  if (andExpr->kind() != Expression::Kind::kLogicalAnd) {
    canBePushed_ = false;
    return;
  }
  auto &andOperands = static_cast<LogicalExpression *>(andExpr)->operands();

  std::vector<Expression *> canBePushExprs;
  std::vector<Expression *> canNotPushExprs;

  for (auto i = 0u; i < andOperands.size(); i++) {
    if (flags[i]) {
      canBePushExprs.emplace_back(andOperands[i]->clone());
    } else {
      canNotPushExprs.emplace_back(andOperands[i]->clone());
    }
  }

  DCHECK(!canBePushExprs.empty());
  DCHECK(!canNotPushExprs.empty());

  hasSplit = true;
  canBePushed_ = true;
  std::vector<Expression *> sharedExprs;
  for (auto i = 0u; i < operands.size(); i++) {
    if (i != canNotPushedIndex) {
      sharedExprs.emplace_back(operands[i]->clone());
    }
  }
  std::vector<Expression *> newExprs;
  newExprs.emplace_back(rewriteExpr(canBePushExprs, sharedExprs));
  newExprs.emplace_back(rewriteExpr(canNotPushExprs, sharedExprs));

  expr->setOperands(std::move(newExprs));
  expr->reverseLogicalKind();
}

Expression *ExtractFilterExprVisitor::rewriteExpr(std::vector<Expression *> rel,
                                                  std::vector<Expression *> sharedExprs) {
  Expression *newAndExpr;
  DCHECK(!rel.empty());
  if (rel.size() > 1) {
    newAndExpr = ExpressionUtils::pushAnds(pool_, rel);
    ExpressionUtils::pullAnds(newAndExpr);
  } else {
    newAndExpr = rel[0];
  }
  if (sharedExprs.empty()) {
    return newAndExpr;
  }
  sharedExprs.emplace_back(newAndExpr);
  auto newOrExpr = ExpressionUtils::pushOrs(pool_, sharedExprs);
  ExpressionUtils::pullOrs(newOrExpr);
  return newOrExpr;
}

// It's recommended to see ExtractFilterExprVisitorTest.cpp to figure out how it works
void ExtractFilterExprVisitor::visit(LogicalExpression *expr) {
  if (expr->operands().size() == 1) {
    expr->operands()[0]->accept(this);
    return;
  }
  LogicalExpression *originalExpr = static_cast<LogicalExpression *>(expr->clone());
  if (expr->kind() == Expression::Kind::kLogicalAnd) {
    std::vector<bool> unUsed;
    visitLogicalAnd(expr, unUsed);
  } else if (expr->kind() == Expression::Kind::kLogicalOr) {
    bool isSplit = visitLogicalOr(expr);
    if (isSplit) {
      DCHECK_EQ(expr->kind(), Expression::Kind::kLogicalAnd);
      DCHECK_EQ(expr->operands().size(), 2);
      remainedExpr_ = expr->operands()[1]->clone();
      remainedExprFromAnd_ = false;
      expr->operands().pop_back();
    }
  } else {
    isNested_ = true;
    splitForbidden = true;
    ExprVisitorImpl::visit(expr);
  }

  if (hasSplit && !canBePushed_) {
    std::vector<Expression *> operands;
    for (auto &operand : originalExpr->operands()) {
      operands.emplace_back(operand->clone());
    }
    expr->setOperands(std::move(operands));
    if (expr->kind() != originalExpr->kind()) {
      expr->reverseLogicalKind();
    }
    remainedExpr_ = nullptr;
  }
  return;
}

void ExtractFilterExprVisitor::visit(SubscriptRangeExpression *) {
  canBePushed_ = false;
}

}  // namespace graph
}  // namespace nebula
