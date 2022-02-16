/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/ArgumentFinder.h"

#include <string>         // for string, basic_...
#include <unordered_set>  // for unordered_set
#include <utility>        // for move

#include "common/base/Base.h"                          // for UNUSED
#include "common/base/Status.h"                        // for Status, NG_RET...
#include "common/expression/FunctionCallExpression.h"  // for ArgumentList
#include "common/expression/PropertyExpression.h"      // for InputPropertyE...
#include "graph/context/QueryContext.h"                // for QueryContext
#include "graph/context/Symbols.h"                     // for SymbolTable
#include "graph/context/ast/CypherAstContext.h"        // for NodeContext
#include "graph/planner/plan/ExecutionPlan.h"          // for SubPlan
#include "graph/planner/plan/Logic.h"                  // for Argument

namespace nebula {
namespace graph {
bool ArgumentFinder::matchNode(NodeContext* nodeCtx) {
  return nodeCtx->nodeAliasesAvailable->count(nodeCtx->info->alias) > 0;
}

bool ArgumentFinder::matchEdge(EdgeContext* nodeCtx) {
  // No need to implement.
  UNUSED(nodeCtx);
  return false;
}

StatusOr<SubPlan> ArgumentFinder::transformNode(NodeContext* nodeCtx) {
  SubPlan subplan;
  auto alias = nodeCtx->info->alias;
  auto argNode = Argument::make(nodeCtx->matchClauseCtx->qctx, alias);
  argNode->setColNames({alias});
  auto aliasGeneratedBy = nodeCtx->matchClauseCtx->qctx->symTable()->getAliasGeneratedBy(alias);
  NG_RETURN_IF_ERROR(aliasGeneratedBy);
  argNode->setInputVar(aliasGeneratedBy.value());
  subplan.root = argNode;
  subplan.tail = argNode;

  // initialize start expression in project node
  auto* pool = nodeCtx->matchClauseCtx->qctx->objPool();
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
