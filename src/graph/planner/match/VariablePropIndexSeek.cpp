/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/VariablePropIndexSeek.h"

#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

namespace nebula {
namespace graph {

bool VariablePropIndexSeek::matchNode(NodeContext* nodeCtx) {
  const auto& nodeInfo = *DCHECK_NOTNULL(nodeCtx->info);
  const auto& labels = nodeInfo.labels;
  if (labels.size() != 1) {
    // TODO multiple tag index seek need the IndexScan support
    VLOG(2) << "Multiple tag index seek is not supported now.";
    return false;
  }

  auto whereClause = nodeCtx->bindWhereClause;
  if (!whereClause || !whereClause->filter) return false;

  std::string refVarName;
  // if (!extractPropIndexVariable(&refVarName)) {
  //   return false;
  // }

  if (!nodeCtx->aliasesAvailable->count(refVarName)) {
    return false;
  }

  nodeCtx->refVarName = refVarName;

  // nodeCtx->scanInfo.filter = filter;
  nodeCtx->scanInfo.schemaIds = nodeInfo.tids;
  nodeCtx->scanInfo.schemaNames = nodeInfo.labels;

  return true;
}

StatusOr<SubPlan> VariablePropIndexSeek::transformNode(NodeContext* nodeCtx) {
  SubPlan plan;

  const auto& schemaIds = nodeCtx->scanInfo.schemaIds;
  DCHECK_EQ(schemaIds.size(), 1u) << "Not supported multiple tag properties seek.";

  auto qctx = nodeCtx->qctx;
  auto argument = Argument::make(qctx, nodeCtx->refVarName);
  argument->setColNames({nodeCtx->refVarName});
  using IQC = nebula::storage::cpp2::IndexQueryContext;
  IQC iqctx;
  iqctx.filter_ref() = Expression::encode(*nodeCtx->scanInfo.filter);
  auto scan =
      IndexScan::make(qctx, argument, nodeCtx->spaceId, {iqctx}, {kVid}, false, schemaIds.back());
  scan->setColNames({kVid});
  plan.tail = argument;
  plan.root = scan;

  // initialize start expression in project node
  auto* pool = nodeCtx->qctx->objPool();
  nodeCtx->initialExpr = VariablePropertyExpression::make(pool, "", kVid);
  return plan;
}

}  // namespace graph
}  // namespace nebula
