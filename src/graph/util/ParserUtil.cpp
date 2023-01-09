// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/util/ParserUtil.h"

#include "common/base/ObjectPool.h"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace graph {

// static
void ParserUtil::rewriteLC(QueryContext *qctx,
                           ListComprehensionExpression *lc,
                           const std::string &oldVarName) {
  // The inner variable will be same with other inner variable in other expression,
  // but the variable is stored in same variable map
  // So to avoid conflict, we create a global unique anonymous variable name for it
  // TODO store inner variable in inner
  const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
  auto *pool = qctx->objPool();

  auto matcher = [](const Expression *expr) -> bool {
    return expr->kind() == Expression::Kind::kLabel ||
           expr->kind() == Expression::Kind::kLabelAttribute ||
           expr->kind() == Expression::Kind::kMatchPathPattern;
  };

  auto rewriter = [&, pool, newVarName](const Expression *expr) {
    Expression *ret = nullptr;
    switch (expr->kind()) {
      case Expression::Kind::kLabel: {
        auto *label = static_cast<const LabelExpression *>(expr);
        if (label->name() == oldVarName) {
          ret = VariableExpression::makeInner(pool, newVarName);
        } else {
          ret = label->clone();
        }
      } break;
      case Expression::Kind::kLabelAttribute: {
        DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
        auto *la = static_cast<const LabelAttributeExpression *>(expr);
        if (la->left()->name() == oldVarName) {
          const auto &value = la->right()->value();
          ret = AttributeExpression::make(pool,
                                          VariableExpression::makeInner(pool, newVarName),
                                          ConstantExpression::make(pool, value));
        } else {
          ret = la->clone();
        }
      } break;
      case Expression::Kind::kMatchPathPattern: {
        auto *mpp = static_cast<MatchPathPatternExpression *>(expr->clone());
        auto &matchPath = mpp->matchPath();
        for (auto &node : matchPath.nodes()) {
          if (node->alias() == oldVarName) {
            node->setAlias(newVarName);
          }
        }
        return static_cast<Expression *>(mpp);
      } break;
      default:
        DLOG(FATAL) << "Unexpected expression kind: " << expr->kind();
    }
    return ret;
  };

  lc->setOriginString(lc->toString());
  lc->setInnerVar(newVarName);
  if (lc->hasFilter()) {
    Expression *filter = lc->filter();
    auto *newFilter = RewriteVisitor::transform(filter, matcher, rewriter);
    lc->setFilter(newFilter);
  }
  if (lc->hasMapping()) {
    Expression *mapping = lc->mapping();
    auto *newMapping = RewriteVisitor::transform(mapping, std::move(matcher), std::move(rewriter));
    lc->setMapping(newMapping);
  }
}

// static
void ParserUtil::rewritePred(QueryContext *qctx,
                             PredicateExpression *pred,
                             const std::string &oldVarName) {
  const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
  auto *pool = qctx->objPool();

  auto matcher = [](const Expression *expr) -> bool {
    return expr->kind() == Expression::Kind::kLabel ||
           expr->kind() == Expression::Kind::kLabelAttribute;
  };

  auto rewriter = [&](const Expression *expr) {
    Expression *ret = nullptr;
    if (expr->kind() == Expression::Kind::kLabel) {
      auto *label = static_cast<const LabelExpression *>(expr);
      if (label->name() == oldVarName) {
        ret = VariableExpression::makeInner(pool, newVarName);
      } else {
        ret = label->clone();
      }
    } else {
      DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
      auto *la = static_cast<const LabelAttributeExpression *>(expr);
      if (la->left()->name() == oldVarName) {
        const auto &value = la->right()->value();
        ret = AttributeExpression::make(pool,
                                        VariableExpression::makeInner(pool, newVarName),
                                        ConstantExpression::make(pool, value));
      } else {
        ret = la->clone();
      }
    }
    return ret;
  };

  pred->setOriginString(pred->toString());
  pred->setInnerVar(newVarName);

  auto *newFilter =
      RewriteVisitor::transform(pred->filter(), std::move(matcher), std::move(rewriter));
  pred->setFilter(newFilter);
}

// static
void ParserUtil::rewriteReduce(QueryContext *qctx,
                               ReduceExpression *reduce,
                               const std::string &oldAccName,
                               const std::string &oldVarName) {
  const auto &newAccName = qctx->vctx()->anonVarGen()->getVar();
  const auto &newVarName = qctx->vctx()->anonVarGen()->getVar();
  auto *pool = qctx->objPool();

  auto matcher = [](const Expression *expr) -> bool {
    return expr->kind() == Expression::Kind::kLabel ||
           expr->kind() == Expression::Kind::kLabelAttribute;
  };
  auto rewriter = [&](const Expression *expr) {
    Expression *ret = nullptr;
    if (expr->kind() == Expression::Kind::kLabel) {
      auto *label = static_cast<const LabelExpression *>(expr);
      if (label->name() == oldAccName) {
        ret = VariableExpression::makeInner(pool, newAccName);
      } else if (label->name() == oldVarName) {
        ret = VariableExpression::makeInner(pool, newVarName);
      } else {
        ret = label->clone();
      }
    } else {
      DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
      auto *la = static_cast<const LabelAttributeExpression *>(expr);
      if (la->left()->name() == oldAccName) {
        const auto &value = la->right()->value();
        ret = AttributeExpression::make(pool,
                                        VariableExpression::makeInner(pool, newAccName),
                                        ConstantExpression::make(pool, value));
      } else if (la->left()->name() == oldVarName) {
        const auto &value = la->right()->value();
        ret = AttributeExpression::make(pool,
                                        VariableExpression::makeInner(pool, newVarName),
                                        ConstantExpression::make(pool, value));
      } else {
        ret = la->clone();
      }
    }
    return ret;
  };

  reduce->setOriginString(reduce->toString());
  reduce->setAccumulator(newAccName);
  reduce->setInnerVar(newVarName);

  auto *newMapping =
      RewriteVisitor::transform(reduce->mapping(), std::move(matcher), std::move(rewriter));
  reduce->setMapping(newMapping);
}

}  // namespace graph
}  // namespace nebula
