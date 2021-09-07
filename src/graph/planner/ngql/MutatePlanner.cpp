/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "graph/planner/ngql/MutatePlanner.h"

#include "graph/planner/plan/Mutate.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> InsertVerticesPlanner::transform(AstContext* astCtx) {
  insertCtx_ = static_cast<InsertVerticesContext*>(astCtx);
  auto& spaceID = insertCtx_->space.id;

  auto* insertNode = InsertVertices::make(insertCtx_->qctx,
                                          nullptr,
                                          spaceID,
                                          std::move(insertCtx_->vertices),
                                          std::move(insertCtx_->tagPropNames),
                                          insertCtx_->ifNotExist);
  SubPlan subPlan;
  subPlan.root = subPlan.tail = insertNode;
  return subPlan;
}

StatusOr<SubPlan> InsertEdgesPlanner::transform(AstContext* astCtx) {
  insertCtx_ = static_cast<InsertEdgesContext*>(astCtx);
  auto& space = insertCtx_->space;

  using IsoLevel = meta::cpp2::IsolationLevel;
  auto isoLevel = space.spaceDesc.isolation_level_ref().value_or(IsoLevel::DEFAULT);
  auto useChainInsert = isoLevel == IsoLevel::TOSS;

  auto* insertNode = InsertEdges::make(insertCtx_->qctx,
                                       nullptr,
                                       space.id,
                                       std::move(insertCtx_->edges),
                                       std::move(insertCtx_->propNames),
                                       insertCtx_->ifNotExist,
                                       useChainInsert);
  SubPlan subPlan;
  subPlan.root = subPlan.tail = insertNode;
  return subPlan;
}

}  //  namespace graph
}  //  namespace nebula
