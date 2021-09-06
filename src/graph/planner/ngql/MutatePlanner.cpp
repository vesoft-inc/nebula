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
  auto qctx = insertCtx_->qctx;
  auto& spaceID = insertCtx_->space.id;

  auto* insertNode = InsertVertices::make(qctx,
                                          nullptr,
                                          spaceID,
                                          std::move(insertCtx_->vertices),
                                          std::move(insertCtx_->tagPropNames),
                                          insertCtx_->ifNotExist);
  SubPlan subPlan;
  subPlan.root = subPlan.tail = insertNode;
  return subPlan;
}

}  //  namespace graph
}  //  namespace nebula
