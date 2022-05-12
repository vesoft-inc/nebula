/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/SegmentsConnector.h"

#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

SubPlan SegmentsConnector::innerJoin(QueryContext* qctx,
                                     const SubPlan& left,
                                     const SubPlan& right,
                                     const std::unordered_set<std::string>& intersectedAliases) {
  SubPlan newPlan = left;
  auto innerJoin = BiInnerJoin::make(qctx, left.root, right.root);
  std::vector<Expression*> hashKeys;
  std::vector<Expression*> probeKeys;
  auto pool = qctx->objPool();
  for (auto& alias : intersectedAliases) {
    auto* args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, alias));
    auto* expr = FunctionCallExpression::make(pool, "id", args);
    hashKeys.emplace_back(expr);
    probeKeys.emplace_back(expr->clone());
  }
  innerJoin->setHashKeys(std::move(hashKeys));
  innerJoin->setProbeKeys(std::move(probeKeys));

  newPlan.root = innerJoin;
  return newPlan;
}

SubPlan SegmentsConnector::leftJoin(QueryContext* qctx,
                                    const SubPlan& left,
                                    const SubPlan& right,
                                    const std::unordered_set<std::string>& intersectedAliases) {
  SubPlan newPlan = left;
  auto leftJoin = BiLeftJoin::make(qctx, left.root, right.root);
  std::vector<Expression*> hashKeys;
  std::vector<Expression*> probeKeys;
  auto pool = qctx->objPool();
  for (auto& alias : intersectedAliases) {
    auto* args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, alias));
    auto* expr = FunctionCallExpression::make(pool, "id", args);
    hashKeys.emplace_back(expr);
    probeKeys.emplace_back(expr->clone());
  }
  leftJoin->setHashKeys(std::move(hashKeys));
  leftJoin->setProbeKeys(std::move(probeKeys));

  newPlan.root = leftJoin;
  return newPlan;
}

SubPlan SegmentsConnector::cartesianProduct(QueryContext* qctx,
                                            const SubPlan& left,
                                            const SubPlan& right) {
  SubPlan newPlan = left;
  newPlan.root = BiCartesianProduct::make(qctx, left.root, right.root);
  return newPlan;
}

/*static*/ SubPlan SegmentsConnector::rollUpApply(QueryContext* qctx,
                                                  const SubPlan& left,
                                                  const std::vector<std::string>& inputColNames,
                                                  const SubPlan& right,
                                                  const std::vector<std::string>& compareCols,
                                                  const std::string& collectCol) {
  SubPlan newPlan = left;
  std::vector<Expression*> compareProps;
  for (const auto& col : compareCols) {
    compareProps.emplace_back(FunctionCallExpression::make(
        qctx->objPool(), "id", {InputPropertyExpression::make(qctx->objPool(), col)}));
  }
  InputPropertyExpression* collectProp = InputPropertyExpression::make(qctx->objPool(), collectCol);
  auto* rollUpApply = RollUpApply::make(
      qctx, left.root, DCHECK_NOTNULL(right.root), std::move(compareProps), collectProp);
  // Left side input may be nullptr, which will be filled in later
  std::vector<std::string> colNames = left.root != nullptr ? left.root->colNames() : inputColNames;
  colNames.emplace_back(collectCol);
  rollUpApply->setColNames(std::move(colNames));
  newPlan.root = rollUpApply;
  // The tail of subplan will be left most RollUpApply
  newPlan.tail = (newPlan.tail == nullptr ? rollUpApply : newPlan.tail);
  return newPlan;
}

SubPlan SegmentsConnector::addInput(const SubPlan& left, const SubPlan& right, bool copyColNames) {
  if (left.root == nullptr) {
    return right;
  }
  SubPlan newPlan = left;
  DCHECK(left.root->isSingleInput());
  if (left.tail->isSingleInput()) {
    auto* mutableLeft = const_cast<PlanNode*>(left.tail);
    auto* siLeft = static_cast<SingleInputNode*>(mutableLeft);
    siLeft->dependsOn(const_cast<PlanNode*>(right.root));
    siLeft->setInputVar(right.root->outputVar());
    if (copyColNames) {
      siLeft->setColNames(right.root->colNames());
    } else if (siLeft->kind() == PlanNode::Kind::kUnwind) {
      // An unwind bypass all aliases, so merge the columns here
      auto colNames = right.root->colNames();
      colNames.insert(colNames.end(), siLeft->colNames().begin(), siLeft->colNames().end());
      siLeft->setColNames(std::move(colNames));
    }
  } else if (left.tail->isBiInput()) {
    auto* mutableLeft = const_cast<PlanNode*>(left.tail);
    auto* siLeft = static_cast<BinaryInputNode*>(mutableLeft);
    siLeft->setLeftDep(const_cast<PlanNode*>(right.root));
    siLeft->setLeftVar(right.root->outputVar());
  } else {
    DLOG(FATAL) << "Unsupported plan node: " << left.tail->kind();
    return newPlan;
  }
  newPlan.tail = right.tail;
  return newPlan;
}
}  // namespace graph
}  // namespace nebula
