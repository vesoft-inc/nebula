/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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
    // Remove left tail kStart plannode before append plan.
    // It allows that kUse sentence to append kMatch Sentence.
    // For example: Use ...; Match ...
    rmLeftTailStartNode((iter + 1)->get(), iter->get()->sentence()->kind());
    NG_RETURN_IF_ERROR((iter + 1)->get()->appendPlan(iter->get()->root()));
  }
  if (validators.front()->tail()->isSingleInput()) {
    subPlan.tail = seqCtx->startNode;
    NG_RETURN_IF_ERROR(validators.front()->appendPlan(subPlan.tail));
  } else {
    subPlan.tail = validators.front()->tail();
  }
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

// When appending plans, it need to remove left tail plannode.
// Because the left tail plannode is StartNode which needs to be removed,
// and remain one size for add dependency
// TODO: It's a temporary solution, remove it after Execute multiple sequences one by one.
void SequentialPlanner::rmLeftTailStartNode(Validator* validator, Sentence::Kind appendPlanKind) {
  if (appendPlanKind != Sentence::Kind::kUse ||
      validator->tail()->kind() != PlanNode::Kind::kStart ||
      validator->root()->dependencies().size() == 0UL) {
    return;
  }

  PlanNode* node = validator->root();
  while (node->dependencies()[0]->dependencies().size() > 0UL) {
    node = const_cast<PlanNode*>(node->dependencies()[0]);
  }
  if (node->dependencies().size() == 1UL) {
    // Remain one size for add dependency
    node->dependencies()[0] = nullptr;
    validator->setTail(node);
  }
}
}  // namespace graph
}  // namespace nebula
