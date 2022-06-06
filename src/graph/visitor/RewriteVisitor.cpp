/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {

void RewriteVisitor::visit(TypeCastingExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  if (matcher_(expr->operand())) {
    auto ret = rewriter_(expr->operand());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setOperand(ret);
  } else {
    expr->operand()->accept(this);
  }
}

void RewriteVisitor::visit(UnaryExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  if (matcher_(expr->operand())) {
    auto ret = rewriter_(expr->operand());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setOperand(ret);
  } else {
    expr->operand()->accept(this);
  }
}

void RewriteVisitor::visit(FunctionCallExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  for (auto &arg : expr->args()->args()) {
    if (matcher_(arg)) {
      auto ret = rewriter_(arg);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      arg = ret;
    } else {
      arg->accept(this);
    }
  }
}

void RewriteVisitor::visit(AggregateExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  auto arg = expr->arg();
  if (matcher_(arg)) {
    auto ret = rewriter_(arg);
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setArg(ret);
  } else {
    arg->accept(this);
  }
}

void RewriteVisitor::visit(LogicalExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  for (auto &operand : expr->operands()) {
    if (matcher_(operand)) {
      auto ret = rewriter_(operand);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      const_cast<Expression *&>(operand) = ret;
    } else {
      operand->accept(this);
    }
  }
}

void RewriteVisitor::visit(ListExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  auto &items = expr->items();
  for (auto &item : items) {
    if (matcher_(item)) {
      auto ret = rewriter_(item);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      const_cast<Expression *&>(item) = ret;
    } else {
      item->accept(this);
    }
  }
}

void RewriteVisitor::visit(SetExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  auto &items = expr->items();
  for (auto &item : items) {
    if (matcher_(item)) {
      auto ret = rewriter_(item);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      const_cast<Expression *&>(item) = ret;
    } else {
      item->accept(this);
    }
  }
}

void RewriteVisitor::visit(MapExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  auto &items = expr->items();
  for (auto &pair : items) {
    auto &item = pair.second;
    if (matcher_(item)) {
      auto ret = rewriter_(item);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      const_cast<Expression *&>(item) = ret;
    } else {
      item->accept(this);
    }
  }
}

void RewriteVisitor::visit(CaseExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  if (expr->hasCondition()) {
    if (matcher_(expr->condition())) {
      auto ret = rewriter_(expr->condition());
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setCondition(ret);
    } else {
      expr->condition()->accept(this);
    }
  }
  if (expr->hasDefault()) {
    if (matcher_(expr->defaultResult())) {
      auto ret = rewriter_(expr->defaultResult());
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setDefault(ret);
    } else {
      expr->defaultResult()->accept(this);
    }
  }
  auto &cases = expr->cases();
  for (size_t i = 0; i < cases.size(); ++i) {
    auto when = cases[i].when;
    auto then = cases[i].then;
    if (matcher_(when)) {
      auto ret = rewriter_(when);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setWhen(i, ret);
    } else {
      when->accept(this);
    }
    if (matcher_(then)) {
      auto ret = rewriter_(then);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setThen(i, ret);
    } else {
      then->accept(this);
    }
  }
}

void RewriteVisitor::visit(ListComprehensionExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  if (matcher_(expr->collection())) {
    auto ret = rewriter_(expr->collection());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setCollection(ret);
  } else {
    expr->collection()->accept(this);
  }
  if (expr->hasFilter()) {
    if (matcher_(expr->filter())) {
      auto ret = rewriter_(expr->filter());
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setFilter(ret);
    } else {
      expr->filter()->accept(this);
    }
  }
  if (expr->hasMapping()) {
    if (matcher_(expr->mapping())) {
      auto ret = rewriter_(expr->mapping());
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setMapping(ret);
    } else {
      expr->mapping()->accept(this);
    }
  }
}

void RewriteVisitor::visit(PredicateExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  if (matcher_(expr->collection())) {
    auto ret = rewriter_(expr->collection());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setCollection(ret);
  } else {
    expr->collection()->accept(this);
  }
  if (expr->hasFilter()) {
    if (matcher_(expr->filter())) {
      auto ret = rewriter_(expr->filter());
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setFilter(ret);
    } else {
      expr->filter()->accept(this);
    }
  }
}

void RewriteVisitor::visit(ReduceExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  if (matcher_(expr->initial())) {
    auto ret = rewriter_(expr->initial());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setInitial(ret);
  } else {
    expr->initial()->accept(this);
  }
  if (matcher_(expr->collection())) {
    auto ret = rewriter_(expr->collection());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setCollection(ret);
  } else {
    expr->collection()->accept(this);
  }
  if (matcher_(expr->mapping())) {
    auto ret = rewriter_(expr->mapping());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setMapping(ret);
  } else {
    expr->mapping()->accept(this);
  }
}

void RewriteVisitor::visit(PathBuildExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  auto &items = expr->items();
  for (auto &item : items) {
    if (matcher_(item)) {
      auto ret = rewriter_(item);
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      const_cast<Expression *&>(item) = ret;
    } else {
      item->accept(this);
    }
  }
}

void RewriteVisitor::visit(LabelTagPropertyExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }
  auto label = expr->label();
  if (matcher_(label)) {
    auto ret = rewriter_(label);
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setLabel(ret);
  } else {
    label->accept(this);
  }
}

void RewriteVisitor::visit(AttributeExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  visitBinaryExpr(expr);
}

void RewriteVisitor::visit(ArithmeticExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  visitBinaryExpr(expr);
}

void RewriteVisitor::visit(RelationalExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  visitBinaryExpr(expr);
}

void RewriteVisitor::visit(SubscriptExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }

  visitBinaryExpr(expr);
}

void RewriteVisitor::visit(SubscriptRangeExpression *expr) {
  if (!ok_) {
    return;
  }

  if (!care(expr->kind())) {
    return;
  }
  if (matcher_(expr->list())) {
    auto ret = rewriter_(expr->list());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setList(ret);
  } else {
    expr->list()->accept(this);
  }
  if (expr->lo() != nullptr) {
    if (matcher_(expr->lo())) {
      auto ret = rewriter_(expr->lo());
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setLo(ret);
    } else {
      expr->lo()->accept(this);
    }
  }
  if (expr->hi() != nullptr) {
    if (matcher_(expr->hi())) {
      auto ret = rewriter_(expr->hi());
      if (ret == nullptr) {
        ok_ = false;
        return;
      }
      expr->setHi(ret);
    } else {
      expr->hi()->accept(this);
    }
  }
}

void RewriteVisitor::visitBinaryExpr(BinaryExpression *expr) {
  if (!ok_) {
    return;
  }

  if (matcher_(expr->left())) {
    auto ret = rewriter_(expr->left());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setLeft(ret);
  } else {
    expr->left()->accept(this);
  }
  if (matcher_(expr->right())) {
    auto ret = rewriter_(expr->right());
    if (ret == nullptr) {
      ok_ = false;
      return;
    }
    expr->setRight(ret);
  } else {
    expr->right()->accept(this);
  }
}

bool RewriteVisitor::care(Expression::Kind kind) {
  if (UNLIKELY(!needVisitedTypes_.empty())) {
    for (auto &k : needVisitedTypes_) {
      if (kind == k) {
        return true;
      }
    }
    return false;
  }
  return true;
}

Expression *RewriteVisitor::transform(const Expression *expr, Matcher matcher, Rewriter rewriter) {
  if (matcher(expr)) {
    return rewriter(expr);
  } else {
    RewriteVisitor visitor(std::move(matcher), std::move(rewriter));
    auto *e = const_cast<Expression *>(expr);
    e->accept(&visitor);
    if (!visitor.ok()) {
      return nullptr;
    }
    return e;
  }
}

Expression *RewriteVisitor::transform(
    const Expression *expr,
    Matcher matcher,
    Rewriter rewriter,
    const std::unordered_set<Expression::Kind> &needVisitedTypes) {
  if (matcher(expr)) {
    return rewriter(expr);
  } else {
    RewriteVisitor visitor(std::move(matcher), std::move(rewriter), std::move(needVisitedTypes));
    auto *e = const_cast<Expression *>(expr);
    e->accept(&visitor);
    if (!visitor.ok()) {
      return nullptr;
    }
    return e;
  }
}
}  // namespace graph
}  // namespace nebula
