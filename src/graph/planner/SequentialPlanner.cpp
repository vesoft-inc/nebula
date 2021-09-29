/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/planner/SequentialPlanner.h"

#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/validator/SequentialValidator.h"
#include "parser/Sentence.h"

namespace nebula {
namespace graph {
bool SequentialPlanner::match(AstContext* astCtx) {
  return astCtx->sentence->kind() == Sentence::Kind::kSequential;
}

StatusOr<SubPlan> SequentialPlanner::transform(AstContext* astCtx) {
  SubPlan subPlan;
  auto* seqCtx = static_cast<SequentialAstContext*>(astCtx);
  auto* qctx = seqCtx->qctx;
  const auto& validators = seqCtx->validators;
  subPlan.root = validators.back()->root();
  ifBuildDataCollect(subPlan, qctx);
  for (auto iter = validators.begin(); iter < validators.end() - 1; ++iter) {
    NG_RETURN_IF_ERROR((iter + 1)->get()->appendPlan(iter->get()->root()));
  }
  subPlan.tail = seqCtx->startNode;
  NG_RETURN_IF_ERROR(validators.front()->appendPlan(subPlan.tail));
  VLOG(1) << subPlan;
  return subPlan;
}

void SequentialPlanner::ifBuildDataCollect(SubPlan& subPlan, QueryContext* qctx) {
  switch (subPlan.root->kind()) {
    case PlanNode::Kind::kSort:
    case PlanNode::Kind::kLimit:
    case PlanNode::Kind::kSample:
    case PlanNode::Kind::kDedup:
    case PlanNode::Kind::kUnion:
    case PlanNode::Kind::kUnionAllVersionVar:
    case PlanNode::Kind::kIntersect:
    case PlanNode::Kind::kCartesianProduct:
    case PlanNode::Kind::kMinus:
    case PlanNode::Kind::kFilter: {
      auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kRowBasedMove);
      dc->addDep(subPlan.root);
      dc->setInputVars({subPlan.root->outputVar()});
      dc->setColNames(subPlan.root->colNames());
      subPlan.root = dc;
      break;
    }
    default:
      break;
  }
}
}  // namespace graph
}  // namespace nebula
