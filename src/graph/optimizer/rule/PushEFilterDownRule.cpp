// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/optimizer/rule/PushEFilterDownRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"
#include "graph/visitor/RewriteVisitor.h"

using nebula::Expression;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushEFilterDownRule::kInstance =
    std::unique_ptr<PushEFilterDownRule>(new PushEFilterDownRule());

PushEFilterDownRule::PushEFilterDownRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushEFilterDownRule::pattern() const {
  static Pattern pattern = Pattern::create(PlanNode::Kind::kTraverse);
  return pattern;
}

bool PushEFilterDownRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto traverse = static_cast<const Traverse *>(matched.planNode({0}));
  DCHECK_EQ(traverse->kind(), graph::PlanNode::Kind::kTraverse);
  // The Zero step require don't apply eFilter in GetNeighbors,
  // but 1 step edges require apply eFilter in GetNeighbors,
  // they are confliction, so don't push down when zero step enabled.
  return traverse->eFilter() != nullptr && !traverse->zeroStep();
}

StatusOr<OptRule::TransformResult> PushEFilterDownRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto traverseGroupNode = matched.node;
  auto traverse = static_cast<const Traverse *>(traverseGroupNode->node());
  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();
  auto eFilter = traverse->eFilter()->clone();

  eFilter = rewriteStarEdge(
      eFilter, traverse->space(), *DCHECK_NOTNULL(traverse->edgeProps()), qctx->schemaMng(), pool);
  if (eFilter == nullptr) {
    return TransformResult::noTransform();
  }

  graph::ExtractFilterExprVisitor visitor(pool);
  eFilter->accept(&visitor);
  if (!visitor.ok()) {
    return TransformResult::noTransform();
  }

  auto remainedExpr = std::move(visitor).remainedExpr();
  auto newTraverse = traverse->clone();
  newTraverse->setOutputVar(traverse->outputVar());
  newTraverse->setEdgeFilter(remainedExpr);
  if (newTraverse->filter() != nullptr) {
    newTraverse->setFilter(
        LogicalExpression::makeAnd(pool, newTraverse->filter(), DCHECK_NOTNULL(eFilter)));
  } else {
    newTraverse->setFilter(DCHECK_NOTNULL(eFilter));
  }
  auto newTraverseGroupNode = OptGroupNode::create(ctx, newTraverse, traverseGroupNode->group());
  newTraverseGroupNode->setDeps(traverseGroupNode->dependencies());

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newTraverseGroupNode);
  return result;
}

std::string PushEFilterDownRule::toString() const {
  return "PushEFilterDownRule";
}

/*static*/ Expression *PushEFilterDownRule::rewriteStarEdge(
    Expression *expr,
    GraphSpaceID spaceId,
    const std::vector<storage::cpp2::EdgeProp> &edges,
    meta::SchemaManager *schemaMng,
    ObjectPool *pool) {
  graph::RewriteVisitor::Matcher matcher = [](const Expression *exp) -> bool {
    switch (exp->kind()) {
      case Expression::Kind::kEdgeSrc:
      case Expression::Kind::kEdgeType:
      case Expression::Kind::kEdgeRank:
      case Expression::Kind::kEdgeDst:
      case Expression::Kind::kEdgeProperty:
        break;
      default:
        return false;
    }
    auto *propertyExpr = static_cast<const PropertyExpression *>(exp);
    if (propertyExpr->sym() != "*") {
      return false;
    } else {
      return true;
    }
  };
  graph::RewriteVisitor::Rewriter rewriter =
      [spaceId, &edges, schemaMng, pool](const Expression *exp) -> Expression * {
    auto *propertyExpr = static_cast<const PropertyExpression *>(exp);
    DCHECK_EQ(propertyExpr->sym(), "*");
    DCHECK(!edges.empty());
    Expression *ret = nullptr;
    if (edges.size() == 1) {
      ret = rewriteStarEdge(propertyExpr, spaceId, edges.front(), schemaMng, pool);
      if (ret == nullptr) {
        return nullptr;
      }
    } else {
      ret = LogicalExpression::makeOr(pool);
      std::vector<Expression *> operands;
      operands.reserve(edges.size());
      for (auto &edge : edges) {
        auto reEdgeExp = rewriteStarEdge(propertyExpr, spaceId, edge, schemaMng, pool);
        if (reEdgeExp == nullptr) {
          return nullptr;
        }
        operands.emplace_back(reEdgeExp);
      }
      static_cast<LogicalExpression *>(ret)->setOperands(std::move(operands));
    }
    return ret;
  };
  return graph::RewriteVisitor::transform(expr, matcher, rewriter);
}

/*static*/ Expression *PushEFilterDownRule::rewriteStarEdge(const PropertyExpression *exp,
                                                            GraphSpaceID spaceId,
                                                            const storage::cpp2::EdgeProp &edge,
                                                            meta::SchemaManager *schemaMng,
                                                            ObjectPool *pool) {
  auto edgeNameResult = schemaMng->toEdgeName(spaceId, edge.get_type());
  if (!edgeNameResult.ok()) {
    return nullptr;
  }
  Expression *ret = nullptr;
  switch (exp->kind()) {
    case Expression::Kind::kEdgeSrc:
      ret = EdgeSrcIdExpression::make(pool, std::move(edgeNameResult).value());
      break;
    case Expression::Kind::kEdgeType:
      ret = EdgeTypeExpression::make(pool, std::move(edgeNameResult).value());
      break;
    case Expression::Kind::kEdgeRank:
      ret = EdgeRankExpression::make(pool, std::move(edgeNameResult).value());
      break;
    case Expression::Kind::kEdgeDst:
      ret = EdgeDstIdExpression::make(pool, std::move(edgeNameResult).value());
      break;
    case Expression::Kind::kEdgeProperty:
      ret = EdgePropertyExpression::make(pool, std::move(edgeNameResult).value(), exp->prop());
      break;
    default:
      LOG(FATAL) << "Unexpected expr: " << exp->kind();
  }
  return ret;
}

}  // namespace opt
}  // namespace nebula
