/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/SamplingClausePlanner.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> SamplingClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kSampling) {
    return Status::Error("Not a valid context for SamplingClausePlanner.");
  }
  auto* samplingCtx = static_cast<SamplingClauseContext*>(clauseCtx);

  SubPlan samplingPlan;
  NG_RETURN_IF_ERROR(buildSampling(samplingCtx, samplingPlan));
  return samplingPlan;
}

Status SamplingClausePlanner::buildSampling(SamplingClauseContext* octx, SubPlan& subplan) {
  auto* currentRoot = subplan.root;
  auto* sampling = Sampling::make(octx->qctx, currentRoot, octx->indexedSamplingFactors);
  subplan.root = sampling;
  subplan.tail = sampling;
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
