/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/Logic.h"

#include "interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

PlanNode* StartNode::clone() const {
  auto* newStart = StartNode::make(qctx_);
  newStart->cloneMembers(*this);
  return newStart;
}

void StartNode::cloneMembers(const StartNode& s) {
  PlanNode::cloneMembers(s);
}

PlanNode* Select::clone() const {
  auto* newSelect = Select::make(qctx_, nullptr);
  newSelect->cloneMembers(*this);
  return newSelect;
}

void Select::cloneMembers(const Select& s) {
  BinarySelect::cloneMembers(s);
}

PlanNode* Loop::clone() const {
  auto* newLoop = Loop::make(qctx_, nullptr);
  newLoop->cloneMembers(*this);
  return newLoop;
}

void Loop::cloneMembers(const Loop& s) {
  BinarySelect::cloneMembers(s);
}

PlanNode* PassThroughNode::clone() const {
  auto* newPt = PassThroughNode::make(qctx_, nullptr);
  newPt->cloneMembers(*this);
  return newPt;
}

void PassThroughNode::cloneMembers(const PassThroughNode& s) {
  SingleInputNode::cloneMembers(s);
}

std::unique_ptr<PlanNodeDescription> BinarySelect::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("condition", condition_ ? condition_->toString() : "", desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> Loop::explain() const {
  auto desc = BinarySelect::explain();
  addDescription("loopBody", std::to_string(body_->id()), desc.get());
  return desc;
}

std::unique_ptr<PlanNodeDescription> Select::explain() const {
  auto desc = BinarySelect::explain();
  addDescription("thenBody", std::to_string(if_->id()), desc.get());
  addDescription("elseBody", std::to_string(else_->id()), desc.get());
  return desc;
}

Argument::Argument(QueryContext* qctx, std::string alias) : PlanNode(qctx, Kind::kArgument) {
  alias_ = alias;
  // An argument is a kind of leaf node, it has no dependencies but read a variable.
  inputVars_.emplace_back(nullptr);
}

PlanNode* Argument::clone() const {
  auto* newArg = Argument::make(qctx_, alias_);
  newArg->cloneMembers(*this);
  return newArg;
}

void Argument::cloneMembers(const Argument& arg) {
  PlanNode::cloneMembers(arg);
  alias_ = arg.getAlias();
  isInputVertexRequired_ = arg.isInputVertexRequired();
}

std::unique_ptr<PlanNodeDescription> Argument::explain() const {
  auto desc = PlanNode::explain();
  addDescription("inputVar", inputVar(), desc.get());
  addDescription("alias", alias_, desc.get());
  addDescription("isInputVertexRequired", std::to_string(isInputVertexRequired_), desc.get());
  return desc;
}
}  // namespace graph
}  // namespace nebula
