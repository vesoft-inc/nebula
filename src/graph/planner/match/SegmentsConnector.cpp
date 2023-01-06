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
  auto innerJoin = HashInnerJoin::make(qctx, left.root, right.root);
  std::vector<Expression*> hashKeys;
  std::vector<Expression*> probeKeys;
  auto pool = qctx->objPool();
  for (auto& alias : intersectedAliases) {
    auto* args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, alias));
    // TODO(czp): We should not do that for all data types,
    // the InputPropertyExpression may be any data type
    auto* expr = FunctionCallExpression::make(pool, "_joinkey", args);
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
  auto leftJoin = HashLeftJoin::make(qctx, left.root, right.root);
  std::vector<Expression*> hashKeys;
  std::vector<Expression*> probeKeys;
  auto pool = qctx->objPool();
  for (auto& alias : intersectedAliases) {
    auto* args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, alias));
    auto* expr = FunctionCallExpression::make(pool, "_joinkey", args);
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
  newPlan.root = CrossJoin::make(qctx, left.root, right.root);
  return newPlan;
}

/*static*/
SubPlan SegmentsConnector::rollUpApply(CypherClauseContextBase* ctx,
                                       const SubPlan& left,
                                       const SubPlan& right,
                                       const graph::Path& path) {
  const std::string& collectCol = path.collectVariable;
  auto* qctx = ctx->qctx;

  SubPlan newPlan = left;
  std::vector<Expression*> compareProps;
  for (const auto& col : path.compareVariables) {
    compareProps.emplace_back(FunctionCallExpression::make(
        qctx->objPool(), "_joinkey", {InputPropertyExpression::make(qctx->objPool(), col)}));
  }
  InputPropertyExpression* collectProp = InputPropertyExpression::make(qctx->objPool(), collectCol);
  auto* rollUpApply = RollUpApply::make(
      qctx, left.root, DCHECK_NOTNULL(right.root), std::move(compareProps), collectProp);

  // Left side input may be nullptr, which will be filled in later
  std::vector<std::string> colNames = left.root ? left.root->colNames() : ctx->inputColNames;
  colNames.emplace_back(collectCol);
  rollUpApply->setColNames(std::move(colNames));
  newPlan.root = rollUpApply;
  // The tail of subplan will be left most RollUpApply
  newPlan.tail = (newPlan.tail == nullptr ? rollUpApply : newPlan.tail);
  return newPlan;
}

/*static*/ SubPlan SegmentsConnector::patternApply(CypherClauseContextBase* ctx,
                                                   const SubPlan& left,
                                                   const SubPlan& right,
                                                   const graph::Path& path) {
  SubPlan newPlan = left;
  auto qctx = ctx->qctx;
  std::vector<Expression*> keyProps;
  for (const auto& col : path.compareVariables) {
    keyProps.emplace_back(FunctionCallExpression::make(
        qctx->objPool(), "_joinkey", {InputPropertyExpression::make(qctx->objPool(), col)}));
  }
  auto* patternApply = PatternApply::make(
      qctx, left.root, DCHECK_NOTNULL(right.root), std::move(keyProps), path.isAntiPred);
  // Left side input may be nullptr, which will be filled later
  std::vector<std::string> colNames =
      left.root != nullptr ? left.root->colNames() : ctx->inputColNames;
  patternApply->setColNames(std::move(colNames));
  newPlan.root = patternApply;
  newPlan.tail = (newPlan.tail == nullptr ? patternApply : newPlan.tail);
  return newPlan;
}

SubPlan SegmentsConnector::addInput(const SubPlan& left, const SubPlan& right, bool copyColNames) {
  if (left.root == nullptr) {
    return right;
  }
  SubPlan newPlan = left;

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
