/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownExpandAllRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"
#include "graph/visitor/RewriteVisitor.h"

using nebula::Expression;
using nebula::graph::ExpandAll;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownExpandAllRule::kInstance =
    std::unique_ptr<PushFilterDownExpandAllRule>(new PushFilterDownExpandAllRule());

PushFilterDownExpandAllRule::PushFilterDownExpandAllRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushFilterDownExpandAllRule::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kExpandAll)});
  return pattern;
}

bool PushFilterDownExpandAllRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto expandAll = static_cast<const ExpandAll *>(matched.planNode({0, 0}));
  if (expandAll->minSteps() != expandAll->maxSteps()) {
    return false;
  }
  auto edgeProps = expandAll->edgeProps();
  // if fetching props of edge in ExpandAll, let it go and do more checks in
  // transform. otherwise skip this rule.
  return edgeProps != nullptr && !edgeProps->empty();
}

Expression *rewriteVarProp(Expression *expr, const ExpandAll *expandAll) {
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kVarProperty;
  };
  auto rewriter = [expandAll](const Expression *e) -> Expression * {
    DCHECK_EQ(e->kind(), Expression::Kind::kVarProperty);
    auto colName = static_cast<const VariablePropertyExpression *>(e)->prop();
    auto vertexColumns = expandAll->vertexColumns();
    if (vertexColumns) {
      for (const auto &column : vertexColumns->columns()) {
        if (column->name() == colName) {
          return column->expr();
        }
      }
    }
    auto edgeColumns = expandAll->edgeColumns();
    if (edgeColumns) {
      for (const auto &column : edgeColumns->columns()) {
        if (column->name() == colName) {
          return column->expr();
        }
      }
    }
    return const_cast<Expression *>(e);
  };
  return graph::RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

StatusOr<OptRule::TransformResult> PushFilterDownExpandAllRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto filterGroupNode = matched.node;
  auto expandAllGroupNode = matched.dependencies.front().node;
  auto filter = static_cast<const Filter *>(filterGroupNode->node());
  auto expandAll = static_cast<const ExpandAll *>(expandAllGroupNode->node());
  auto colNames = expandAll->colNames();
  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();
  auto condition = filter->condition()->clone();

  graph::ExtractFilterExprVisitor visitor(pool, colNames);
  condition->accept(&visitor);
  if (!visitor.ok()) {
    return TransformResult::noTransform();
  }

  auto remainedExpr = std::move(visitor).remainedExpr();
  OptGroupNode *newFilterGroupNode = nullptr;
  PlanNode *newFilter = nullptr;
  if (remainedExpr != nullptr) {
    newFilter = Filter::make(qctx, nullptr, remainedExpr);
    newFilter->setOutputVar(filter->outputVar());
    newFilterGroupNode = OptGroupNode::create(ctx, newFilter, filterGroupNode->group());
  }

  // rewrite filter to original expression (edgePropExpression、srcPropExpression)
  auto newFilterExpr = rewriteVarProp(condition, expandAll);
  auto newExpandAll = static_cast<ExpandAll *>(expandAll->clone());
  // push filter down storage
  if (expandAll->filter() != nullptr) {
    auto logicExpr = LogicalExpression::makeAnd(pool, newFilterExpr, expandAll->filter()->clone());
    newFilterExpr = logicExpr;
  }
  newExpandAll->setFilter(newFilterExpr);

  OptGroupNode *newExpandGroupNode = nullptr;
  if (newFilterGroupNode != nullptr) {
    // Filter(A&&B)<-ExpandAll(C) => Filter(A)<-ExpandAll(B&&C)
    auto newGroup = OptGroup::create(ctx);
    newExpandGroupNode = newGroup->makeGroupNode(newExpandAll);
    newFilter->setInputVar(newExpandAll->outputVar());
    newFilterGroupNode->dependsOn(newGroup);
  } else {
    // Filter(A)<-ExpandAll(C) => ExpandAll(A&&C)
    newExpandGroupNode = OptGroupNode::create(ctx, newExpandAll, filterGroupNode->group());
    newExpandAll->setOutputVar(filter->outputVar());
  }

  for (auto dep : expandAllGroupNode->dependencies()) {
    newExpandGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newFilterGroupNode ? newFilterGroupNode : newExpandGroupNode);
  return result;
}

std::string PushFilterDownExpandAllRule::toString() const {
  return "PushFilterDownExpandAllRule";
}

}  // namespace opt
}  // namespace nebula
