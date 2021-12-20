/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/AddInputStrategy.h"

namespace nebula {
namespace graph {
PlanNode* AddInputStrategy::connect(const PlanNode* left, const PlanNode* right) {
  DCHECK(left->isSingleInput());
  auto* mutableLeft = const_cast<PlanNode*>(left);
  auto* siLeft = static_cast<SingleInputNode*>(mutableLeft);
  siLeft->dependsOn(const_cast<PlanNode*>(right));
  siLeft->setInputVar(right->outputVar());
  if (copyColNames_) {
    siLeft->setColNames(right->colNames());
  } else if (siLeft->kind() == PlanNode::Kind::kUnwind) {
    // An unwind bypass all aliases, so merge the columns here
    auto colNames = right->colNames();
    colNames.insert(colNames.end(), siLeft->colNames().begin(), siLeft->colNames().end());
    siLeft->setColNames(std::move(colNames));
  }
  return nullptr;
}
}  // namespace graph
}  // namespace nebula
