/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/FetchEdgesPlanner.h"

#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <set>            // for operator!=, set
#include <string>         // for string, basic_st...
#include <unordered_map>  // for _Node_const_iter...
#include <utility>        // for move, pair

#include "common/base/ObjectPool.h"                  // for ObjectPool
#include "common/datatypes/Value.h"                  // for Value, Value::kE...
#include "common/expression/ConstantExpression.h"    // for ConstantExpression
#include "common/expression/LogicalExpression.h"     // for LogicalExpression
#include "common/expression/PropertyExpression.h"    // for EdgeDstIdExpression
#include "common/expression/RelationalExpression.h"  // for RelationalExpres...
#include "graph/context/QueryContext.h"              // for QueryContext
#include "graph/context/ast/QueryAstContext.h"       // for FetchEdgesContext
#include "graph/planner/plan/ExecutionPlan.h"        // for SubPlan
#include "graph/planner/plan/Query.h"                // for Expr, GetEdges
#include "graph/session/ClientSession.h"             // for SpaceInfo
#include "graph/visitor/DeducePropsVisitor.h"        // for ExpressionProps

namespace nebula {
class Expression;

class Expression;

namespace graph {

std::unique_ptr<FetchEdgesPlanner::EdgeProps> FetchEdgesPlanner::buildEdgeProps() {
  auto eProps = std::make_unique<EdgeProps>();
  const auto &edgePropsMap = fetchCtx_->exprProps.edgeProps();
  for (const auto &edgeProp : edgePropsMap) {
    EdgeProp ep;
    ep.type_ref() = edgeProp.first;
    ep.props_ref() = std::vector<std::string>(edgeProp.second.begin(), edgeProp.second.end());
    eProps->emplace_back(std::move(ep));
  }
  return eProps;
}

Expression *FetchEdgesPlanner::emptyEdgeFilter() {
  auto *pool = fetchCtx_->qctx->objPool();
  const auto &edgeName = fetchCtx_->edgeName;
  auto notEmpty = [&pool](Expression *expr) {
    return RelationalExpression::makeNE(pool, ConstantExpression::make(pool, Value::kEmpty), expr);
  };
  auto exprAnd = [&pool](Expression *left, Expression *right) {
    return LogicalExpression::makeAnd(pool, left, right);
  };

  auto *srcNotEmpty = notEmpty(EdgeSrcIdExpression::make(pool, edgeName));
  auto *dstNotEmpty = notEmpty(EdgeDstIdExpression::make(pool, edgeName));
  auto *rankNotEmpty = notEmpty(EdgeRankExpression::make(pool, edgeName));
  return exprAnd(srcNotEmpty, exprAnd(dstNotEmpty, rankNotEmpty));
}

StatusOr<SubPlan> FetchEdgesPlanner::transform(AstContext *astCtx) {
  fetchCtx_ = static_cast<FetchEdgesContext *>(astCtx);
  auto qctx = fetchCtx_->qctx;
  auto spaceID = fetchCtx_->space.id;

  SubPlan subPlan;
  auto *getEdges = GetEdges::make(qctx,
                                  nullptr,
                                  spaceID,
                                  fetchCtx_->src,
                                  fetchCtx_->type,
                                  fetchCtx_->rank,
                                  fetchCtx_->dst,
                                  buildEdgeProps(),
                                  {},
                                  fetchCtx_->distinct);
  getEdges->setInputVar(fetchCtx_->inputVarName);

  subPlan.root = Filter::make(qctx, getEdges, emptyEdgeFilter());

  subPlan.root = Project::make(qctx, subPlan.root, fetchCtx_->yieldExpr);
  if (fetchCtx_->distinct) {
    subPlan.root = Dedup::make(qctx, subPlan.root);
  }
  subPlan.tail = getEdges;
  return subPlan;
}

}  // namespace graph
}  // namespace nebula
