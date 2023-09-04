/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/Algo.h"

#include "PlanNode.h"
#include "graph/planner/plan/PlanNodeVisitor.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/ToJson.h"
namespace nebula {
namespace graph {
PlanNode* AllPaths::clone() const {
  auto* path = AllPaths::make(qctx_, nullptr, nullptr, space_, steps_, noLoop_, withProp_);
  path->cloneMembers(*this);
  return path;
}

void AllPaths::cloneMembers(const AllPaths& path) {
  BinaryInputNode::cloneMembers(path);
  offset_ = path.offset_;
  limit_ = path.limit_;
  filter_ = path.filter_;
  stepFilter_ = path.stepFilter_;
  if (path.vertexProps_) {
    auto vertexProps = *path.vertexProps_;
    auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(vertexProps);
    setVertexProps(std::move(vertexPropsPtr));
  }
  if (path.edgeProps_) {
    auto edgeProps = *path.edgeProps_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setEdgeProps(std::move(edgePropsPtr));
  }
  if (path.reverseEdgeProps_) {
    auto edgeProps = *path.reverseEdgeProps_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setReverseEdgeProps(std::move(edgePropsPtr));
  }
}

PlanNode* BFSShortestPath::clone() const {
  auto* path = BFSShortestPath::make(qctx_, nullptr, nullptr, steps_);
  path->cloneMembers(*this);
  return path;
}

void BFSShortestPath::cloneMembers(const BFSShortestPath& path) {
  BinaryInputNode::cloneMembers(path);
  singleShortest_ = path.singleShortest_;
  leftVidVar_ = path.leftVidVar_;
  rightVidVar_ = path.rightVidVar_;
  terminateEarlyVar_ = path.terminateEarlyVar_;
  limit_ = path.limit_;
}

PlanNode* MultiShortestPath::clone() const {
  auto* path = MultiShortestPath::make(qctx_, nullptr, nullptr, steps_);
  path->cloneMembers(*this);
  return path;
}

void MultiShortestPath::cloneMembers(const MultiShortestPath& path) {
  BinaryInputNode::cloneMembers(path);
  singleShortest_ = path.singleShortest_;
  leftVidVar_ = path.leftVidVar_;
  rightVidVar_ = path.rightVidVar_;
  terminationVar_ = path.terminationVar_;
  limit_ = path.limit_;
}

std::unique_ptr<PlanNodeDescription> BFSShortestPath::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("LeftNextVidVar", folly::toJson(util::toJson(leftVidVar_)), desc.get());
  addDescription("RightNextVidVar", folly::toJson(util::toJson(rightVidVar_)), desc.get());
  addDescription("steps", folly::toJson(util::toJson(steps_)), desc.get());
  addDescription("singleShortest", folly::toJson(util::toJson(singleShortest_)), desc.get());
  addDescription("limit", folly::toJson(util::toJson(limit_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> MultiShortestPath::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("LeftNextVidVar", folly::toJson(util::toJson(leftVidVar_)), desc.get());
  addDescription("RightNextVidVar", folly::toJson(util::toJson(rightVidVar_)), desc.get());
  addDescription("steps", folly::toJson(util::toJson(steps_)), desc.get());
  addDescription("singleShortest", folly::toJson(util::toJson(singleShortest_)), desc.get());
  addDescription("limit", folly::toJson(util::toJson(limit_)), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> AllPaths::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("noloop ", folly::toJson(util::toJson(noLoop_)), desc.get());
  addDescription("withProp ", folly::toJson(util::toJson(withProp_)), desc.get());
  addDescription("steps", folly::toJson(util::toJson(steps_)), desc.get());
  addDescription("filter", filter_ == nullptr ? "" : filter_->toString(), desc.get());
  addDescription("stepFilter", stepFilter_ == nullptr ? "" : stepFilter_->toString(), desc.get());
  addDescription("offset", folly::toJson(util::toJson(offset_)), desc.get());
  addDescription("limit", folly::toJson(util::toJson(limit_)), desc.get());
  addDescription(
      "vertexProps", vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "", desc.get());
  addDescription(
      "edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "", desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> ShortestPath::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("singleShortest", folly::toJson(util::toJson(singleShortest_)), desc.get());
  addDescription("steps", range_.toString(), desc.get());
  addDescription("edgeDirection", apache::thrift::util::enumNameSafe(edgeDirection_), desc.get());
  addDescription(
      "vertexProps", vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "", desc.get());
  addDescription(
      "edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "", desc.get());
  return desc;
}

PlanNode* ShortestPath::clone() const {
  auto* path = ShortestPath::make(qctx_, nullptr, space_, singleShortest_);
  path->cloneMembers(*this);
  return path;
}

void ShortestPath::cloneMembers(const ShortestPath& path) {
  SingleInputNode::cloneMembers(path);
  setStepRange(path.range_);
  setEdgeDirection(path.edgeDirection_);
  if (path.vertexProps_) {
    auto vertexProps = *path.vertexProps_;
    auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(vertexProps);
    setVertexProps(std::move(vertexPropsPtr));
  }
  if (path.edgeProps_) {
    auto edgeProps = *path.edgeProps_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setEdgeProps(std::move(edgePropsPtr));
  }
  if (path.reverseEdgeProps_) {
    auto edgeProps = *path.reverseEdgeProps_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setReverseEdgeProps(std::move(edgePropsPtr));
  }
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

std::unique_ptr<PlanNodeDescription> Subgraph::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("src", src_ ? src_->toString() : "", desc.get());
  addDescription("tag_filter", tagFilter_ ? tagFilter_->toString() : "", desc.get());
  addDescription("edge_filter", edgeFilter_ ? edgeFilter_->toString() : "", desc.get());
  addDescription("filter", filter_ ? filter_->toString() : "", desc.get());
  addDescription(
      "vertexProps", vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "", desc.get());
  addDescription(
      "edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "", desc.get());
  addDescription(
      "steps", folly::toJson(util::toJson(oneMoreStep_ ? steps_ : steps_ - 1)), desc.get());
  return desc;
}

}  // namespace graph
}  // namespace nebula
