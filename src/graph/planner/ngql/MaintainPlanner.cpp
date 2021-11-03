/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/MaintainPlanner.h"

#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/plan/Maintain.h"

namespace nebula {
namespace graph {

StatusOr<SubPlan> CreateTagPlanner::transform(AstContext* astCtx) {
  auto createCtx = static_cast<CreateSchemaContext*>(astCtx);
  SubPlan plan;
  plan.root = plan.tail = CreateTag::make(createCtx->qctx,
                                          nullptr,
                                          std::move(createCtx->name),
                                          std::move(createCtx->schema),
                                          createCtx->ifNotExist);
  return plan;
}

StatusOr<SubPlan> CreateEdgePlanner::transform(AstContext* astCtx) {
  auto createCtx = static_cast<CreateSchemaContext*>(astCtx);
  SubPlan plan;
  plan.root = plan.tail = CreateEdge::make(createCtx->qctx,
                                           nullptr,
                                           std::move(createCtx->name),
                                           std::move(createCtx->schema),
                                           createCtx->ifNotExist);
  return plan;
}

StatusOr<SubPlan> AlterTagPlanner::transform(AstContext* astCtx) {
  auto alterCtx = static_cast<AlterSchemaContext*>(astCtx);
  auto qctx = alterCtx->qctx;
  auto name = *static_cast<const AlterTagSentence*>(alterCtx->sentence)->name();
  SubPlan plan;
  plan.root = plan.tail = AlterTag::make(qctx,
                                         nullptr,
                                         alterCtx->space.id,
                                         std::move(name),
                                         std::move(alterCtx->schemaItems),
                                         std::move(alterCtx->schemaProps));
  return plan;
}

StatusOr<SubPlan> AlterEdgePlanner::transform(AstContext* astCtx) {
  auto alterCtx = static_cast<AlterSchemaContext*>(astCtx);
  auto qctx = alterCtx->qctx;
  auto name = *static_cast<const AlterEdgeSentence*>(alterCtx->sentence)->name();
  SubPlan plan;
  plan.root = plan.tail = AlterEdge::make(qctx,
                                          nullptr,
                                          alterCtx->space.id,
                                          std::move(name),
                                          std::move(alterCtx->schemaItems),
                                          std::move(alterCtx->schemaProps));
  return plan;
}

}  // namespace graph
}  // namespace nebula
