/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/LookupPlanner.h"

#include "common/base/Base.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/Scan.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

std::unique_ptr<Planner> LookupPlanner::make() {
  return std::unique_ptr<LookupPlanner>(new LookupPlanner());
}

bool LookupPlanner::match(AstContext* astCtx) {
  return astCtx->sentence->kind() == Sentence::Kind::kLookup;
}

StatusOr<SubPlan> LookupPlanner::transform(AstContext* astCtx) {
  auto lookupCtx = static_cast<LookupContext*>(astCtx);
  auto qctx = lookupCtx->qctx;
  auto from = static_cast<const LookupSentence*>(lookupCtx->sentence)->from();
  SubPlan plan;
  if (lookupCtx->isEdge) {
    auto* edgeIndexFullScan = EdgeIndexFullScan::make(qctx,
                                                      nullptr,
                                                      from,
                                                      lookupCtx->space.id,
                                                      {},
                                                      lookupCtx->idxReturnCols,
                                                      lookupCtx->schemaId,
                                                      lookupCtx->isEmptyResultSet);
    edgeIndexFullScan->setYieldColumns(lookupCtx->yieldExpr);
    plan.tail = edgeIndexFullScan;
    plan.root = edgeIndexFullScan;
  } else {
    auto* tagIndexFullScan = TagIndexFullScan::make(qctx,
                                                    nullptr,
                                                    from,
                                                    lookupCtx->space.id,
                                                    {},
                                                    lookupCtx->idxReturnCols,
                                                    lookupCtx->schemaId,
                                                    lookupCtx->isEmptyResultSet);
    tagIndexFullScan->setYieldColumns(lookupCtx->yieldExpr);
    plan.tail = tagIndexFullScan;
    plan.root = tagIndexFullScan;
  }
  plan.tail->setColNames(lookupCtx->idxColNames);

  if (lookupCtx->filter) {
    plan.root = Filter::make(qctx, plan.root, lookupCtx->filter);
  }

  plan.root = Project::make(qctx, plan.root, lookupCtx->yieldExpr);
  return plan;
}

}  // namespace graph
}  // namespace nebula
