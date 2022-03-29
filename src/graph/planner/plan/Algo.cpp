/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/Algo.h"

#include "graph/util/ToJson.h"
namespace nebula {
namespace graph {

std::unique_ptr<PlanNodeDescription> BFSShortestPath::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("LeftNextVidVar", util::toJson(leftVidVar_), desc.get());
  addDescription("RightNextVidVar", util::toJson(rightVidVar_), desc.get());
  addDescription("steps", util::toJson(steps_), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> MultiShortestPath::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("LeftNextVidVar", util::toJson(leftVidVar_), desc.get());
  addDescription("RightNextVidVar", util::toJson(rightVidVar_), desc.get());
  addDescription("steps", util::toJson(steps_), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ProduceAllPaths::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("LeftNextVidVar", util::toJson(leftVidVar_), desc.get());
  addDescription("RightNextVidVar", util::toJson(rightVidVar_), desc.get());
  addDescription("noloop ", util::toJson(noLoop_), desc.get());
  addDescription("steps", util::toJson(steps_), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> CartesianProduct::explain() const {
  auto desc = SingleDependencyNode::explain();
  for (size_t i = 0; i < inputVars_.size(); ++i) {
    addDescription("var", folly::toJson(util::toJson(inputVars_[i])), desc.get());
  }
  return desc;
}

Status CartesianProduct::addVar(std::string varName) {
  auto checkName = [&varName](auto var) { return var->name == varName; };
  if (std::find_if(inputVars_.begin(), inputVars_.end(), checkName) != inputVars_.end()) {
    return Status::SemanticError("Duplicate Var: %s", varName.c_str());
  }
  auto* varPtr = qctx_->symTable()->getVar(varName);
  DCHECK(varPtr != nullptr);
  allColNames_.emplace_back(varPtr->colNames);
  inputVars_.emplace_back(varPtr);
  return Status::OK();
}

std::vector<std::string> CartesianProduct::inputVars() const {
  std::vector<std::string> varNames;
  varNames.reserve(inputVars_.size());
  for (auto i : inputVars_) {
    varNames.emplace_back(i->name);
  }
  return varNames;
}

std::unique_ptr<PlanNodeDescription> BiCartesianProduct::explain() const {
  return BinaryInputNode::explain();
}

BiCartesianProduct::BiCartesianProduct(QueryContext* qctx, PlanNode* left, PlanNode* right)
    : BinaryInputNode(qctx, Kind::kBiCartesianProduct, left, right) {
  auto lColNames = left->colNames();
  auto rColNames = right->colNames();
  lColNames.insert(lColNames.end(), rColNames.begin(), rColNames.end());
  setColNames(lColNames);
}
}  // namespace graph
}  // namespace nebula
