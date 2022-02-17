/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/WithClausePlanner.h"

#include <folly/String.h>  // for join
#include <stdint.h>        // for int64_t

#include <limits>       // for numeric_limits
#include <memory>       // for unique_ptr
#include <string>       // for operator<<
#include <type_traits>  // for remove_referen...
#include <utility>      // for move

#include "common/base/Logging.h"                       // for COMPACT_GOOGLE...
#include "graph/context/ast/CypherAstContext.h"        // for WithClauseContext
#include "graph/planner/match/OrderByClausePlanner.h"  // for OrderByClauseP...
#include "graph/planner/match/PaginationPlanner.h"     // for PaginationPlanner
#include "graph/planner/match/SegmentsConnector.h"     // for SegmentsConnector
#include "graph/planner/match/WhereClausePlanner.h"    // for WhereClausePla...
#include "graph/planner/match/YieldClausePlanner.h"    // for YieldClausePla...
#include "graph/planner/plan/ExecutionPlan.h"          // for SubPlan
#include "graph/planner/plan/PlanNode.h"               // for PlanNode

namespace nebula {
namespace graph {
StatusOr<SubPlan> WithClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kWith) {
    return Status::Error("Not a valid context for WithClausePlanner.");
  }
  auto* withClauseCtx = static_cast<WithClauseContext*>(clauseCtx);

  SubPlan withPlan;
  NG_RETURN_IF_ERROR(buildWith(withClauseCtx, withPlan));
  return withPlan;
}

Status WithClausePlanner::buildWith(WithClauseContext* wctx, SubPlan& subPlan) {
  auto yieldPlan = std::make_unique<YieldClausePlanner>()->transform(wctx->yield.get());
  NG_RETURN_IF_ERROR(yieldPlan);
  auto yieldplan = std::move(yieldPlan).value();
  subPlan.tail = yieldplan.tail;
  subPlan.root = yieldplan.root;

  if (wctx->order != nullptr) {
    auto orderPlan = std::make_unique<OrderByClausePlanner>()->transform(wctx->order.get());
    NG_RETURN_IF_ERROR(orderPlan);
    auto plan = std::move(orderPlan).value();
    subPlan = SegmentsConnector::addInput(plan, subPlan, true);
  }

  if (wctx->pagination != nullptr &&
      (wctx->pagination->skip != 0 ||
       wctx->pagination->limit != std::numeric_limits<int64_t>::max())) {
    auto paginationPlan = std::make_unique<PaginationPlanner>()->transform(wctx->pagination.get());
    NG_RETURN_IF_ERROR(paginationPlan);
    auto plan = std::move(paginationPlan).value();
    subPlan = SegmentsConnector::addInput(plan, subPlan, true);
  }

  if (wctx->where != nullptr) {
    bool needStableFilter = wctx->order != nullptr;
    auto wherePlan =
        std::make_unique<WhereClausePlanner>(needStableFilter)->transform(wctx->where.get());
    NG_RETURN_IF_ERROR(wherePlan);
    auto plan = std::move(wherePlan).value();
    subPlan = SegmentsConnector::addInput(plan, subPlan, true);
  }

  VLOG(1) << "with root: " << subPlan.root->outputVar()
          << " colNames: " << folly::join(",", subPlan.root->colNames());

  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
