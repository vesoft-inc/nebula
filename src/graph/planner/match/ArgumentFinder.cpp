/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/ArgumentFinder.h"

#include "graph/planner/plan/Logic.h"

namespace nebula {
namespace graph {
bool ArgumentFinder::matchNode(NodeContext* nodeCtx) {
  return nodeCtx->aliasesAvailable->count(nodeCtx->info->alias) > 0;
}

bool ArgumentFinder::matchEdge(EdgeContext* nodeCtx) {
  // No need to implement.
  UNUSED(nodeCtx);
  return false;
}

StatusOr<SubPlan> ArgumentFinder::transformNode(NodeContext* nodeCtx) {
  SubPlan subplan;
  auto alias = nodeCtx->info->alias;
  auto argNode = Argument::make(nodeCtx->qctx, alias);
  argNode->setColNames({alias});
  subplan.root = argNode;
  subplan.tail = argNode;

  // initialize start expression in project node
  auto* pool = nodeCtx->qctx->objPool();
  auto* args = ArgumentList::make(pool);
  args->addArgument(InputPropertyExpression::make(pool, alias));
  nodeCtx->initialExpr = FunctionCallExpression::make(pool, "id", args);
  return subplan;
}

StatusOr<SubPlan> ArgumentFinder::transformEdge(EdgeContext* edgeCtx) {
  // No need to implement.
  UNUSED(edgeCtx);
  return Status::Error("ArgumentFinder for edge is not implemented.");
}
}  // namespace graph
}  // namespace nebula
