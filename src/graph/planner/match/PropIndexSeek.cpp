/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/PropIndexSeek.h"

#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

namespace nebula {
namespace graph {
bool PropIndexSeek::matchEdge(EdgeContext* edgeCtx) {
  auto& edge = *edgeCtx->info;
  if (edge.types.size() != 1) {
    // TODO multiple edge index seek need the IndexScan support
    VLOG(2) << "Multiple edge index seek is not supported now.";
    return false;
  }

  if (edge.range != nullptr && edge.range->min() == 0) {
    // The 0 step is NodeScan in fact.
    return false;
  }

  Expression* filter = nullptr;
  if (edgeCtx->bindWhereClause != nullptr && edgeCtx->bindWhereClause->filter != nullptr) {
    filter = MatchSolver::makeIndexFilter(
        edge.types.back(), edge.alias, edgeCtx->bindWhereClause->filter, edgeCtx->qctx, true);
  }
  if (filter == nullptr) {
    if (edge.props != nullptr && !edge.props->items().empty()) {
      filter = MatchSolver::makeIndexFilter(edge.types.back(), edge.props, edgeCtx->qctx, true);
    }
  }

  if (filter == nullptr) {
    return false;
  }

  edgeCtx->scanInfo.filter = filter;
  edgeCtx->scanInfo.schemaIds = edge.edgeTypes;
  edgeCtx->scanInfo.schemaNames = edge.types;
  edgeCtx->scanInfo.direction = edge.direction;

  return true;
}

StatusOr<SubPlan> PropIndexSeek::transformEdge(EdgeContext* edgeCtx) {
  SubPlan plan;
  DCHECK_EQ(edgeCtx->scanInfo.schemaIds.size(), 1)
      << "Not supported multiple edge properties seek.";
  using IQC = nebula::storage::cpp2::IndexQueryContext;
  IQC iqctx;
  iqctx.filter_ref() = Expression::encode(*edgeCtx->scanInfo.filter);
  std::vector<std::string> columns, columnsName;
  switch (edgeCtx->scanInfo.direction) {
    case MatchEdge::Direction::OUT_EDGE:
      columns.emplace_back(kSrc);
      columnsName.emplace_back(kVid);
      break;
    case MatchEdge::Direction::IN_EDGE:
      columns.emplace_back(kDst);
      columnsName.emplace_back(kVid);
      break;
    case MatchEdge::Direction::BOTH:
      columns.emplace_back(kSrc);
      columns.emplace_back(kDst);
      columnsName.emplace_back(kSrc);
      columnsName.emplace_back(kDst);
      break;
  }

  auto* qctx = edgeCtx->qctx;
  auto scan = IndexScan::make(qctx,
                              nullptr,
                              edgeCtx->spaceId,
                              {iqctx},
                              std::move(columns),
                              true,
                              edgeCtx->scanInfo.schemaIds.back());
  scan->setColNames(columnsName);
  plan.tail = scan;
  plan.root = scan;

  auto* pool = qctx->objPool();
  if (edgeCtx->scanInfo.direction == MatchEdge::Direction::BOTH) {
    PlanNode* left = nullptr;
    {
      auto* yieldColumns = pool->makeAndAdd<YieldColumns>();
      yieldColumns->addColumn(new YieldColumn(InputPropertyExpression::make(pool, kSrc)));
      left = Project::make(qctx, scan, yieldColumns);
      left->setColNames({kVid});
    }
    PlanNode* right = nullptr;
    {
      auto* yieldColumns = pool->makeAndAdd<YieldColumns>();
      yieldColumns->addColumn(new YieldColumn(InputPropertyExpression::make(pool, kDst)));
      right = Project::make(qctx, scan, yieldColumns);
      right->setColNames({kVid});
    }

    plan.root = Union::make(qctx, left, right);
    plan.root->setColNames({kVid});
  }

  auto* dedup = Dedup::make(qctx, plan.root);
  dedup->setColNames(plan.root->colNames());
  plan.root = dedup;

  // initialize start expression in project edge
  edgeCtx->initialExpr = VariablePropertyExpression::make(pool, "", kVid);
  return plan;
}

bool PropIndexSeek::matchNode(NodeContext* nodeCtx) {
  auto& node = *nodeCtx->info;
  if (node.labels.size() != 1) {
    // TODO multiple tag index seek need the IndexScan support
    VLOG(2) << "Multiple tag index seek is not supported now.";
    return false;
  }

  Expression* filterInWhere = nullptr;
  Expression* filterInPattern = nullptr;
  if (nodeCtx->bindWhereClause != nullptr && nodeCtx->bindWhereClause->filter != nullptr) {
    filterInWhere = MatchSolver::makeIndexFilter(
        node.labels.back(), node.alias, nodeCtx->bindWhereClause->filter, nodeCtx->qctx);
  }
  if (!node.labelProps.empty()) {
    auto props = node.labelProps.back();
    if (props != nullptr) {
      filterInPattern = MatchSolver::makeIndexFilter(node.labels.back(), props, nodeCtx->qctx);
    }
  }

  Expression* filter = nullptr;
  if (!filterInPattern && !filterInWhere) {
    return false;
  } else if (!filterInPattern) {
    filter = filterInWhere;
  } else if (!filterInWhere) {
    filter = filterInPattern;
  } else {
    filter = LogicalExpression::makeAnd(nodeCtx->qctx->objPool(), filterInPattern, filterInWhere);
  }

  nodeCtx->scanInfo.filter = filter;
  nodeCtx->scanInfo.schemaIds = node.tids;
  nodeCtx->scanInfo.schemaNames = node.labels;

  return true;
}

StatusOr<SubPlan> PropIndexSeek::transformNode(NodeContext* nodeCtx) {
  SubPlan plan;
  DCHECK_EQ(nodeCtx->scanInfo.schemaIds.size(), 1) << "Not supported multiple tag properties seek.";
  using IQC = nebula::storage::cpp2::IndexQueryContext;
  IQC iqctx;
  iqctx.filter_ref() = Expression::encode(*nodeCtx->scanInfo.filter);
  auto scan = IndexScan::make(nodeCtx->qctx,
                              nullptr,
                              nodeCtx->spaceId,
                              {iqctx},
                              {kVid},
                              false,
                              nodeCtx->scanInfo.schemaIds.back());
  scan->setColNames({kVid});
  plan.tail = scan;
  plan.root = scan;

  // initialize start expression in project node
  auto* pool = nodeCtx->qctx->objPool();
  nodeCtx->initialExpr = VariablePropertyExpression::make(pool, "", kVid);
  return plan;
}

}  // namespace graph
}  // namespace nebula
