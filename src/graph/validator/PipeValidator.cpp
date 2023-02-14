/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/PipeValidator.h"

#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

// Validate pipe sentence which combine two sentence by input/output, e.g. GO ... | LIMIT 3
// Set input, and call validator of sub-sentence.
Status PipeValidator::validateImpl() {
  lValidator_->setInputCols(std::move(inputs_));
  lValidator_->setInputVarName(inputVarName_);
  NG_RETURN_IF_ERROR(lValidator_->validate());

  rValidator_->setInputCols(lValidator_->outputCols());
  rValidator_->setInputVarName(lValidator_->root()->outputVar());
  NG_RETURN_IF_ERROR(rValidator_->validate());

  outputs_ = rValidator_->outputCols();
  return Status::OK();
}

// Concat two sub-plans by input/output
Status PipeValidator::toPlan() {
  root_ = rValidator_->root();
  tail_ = lValidator_->tail();
  if (rValidator_->tail()->kind() == PlanNode::Kind::kArgument) {
    return Status::OK();
  }
  NG_RETURN_IF_ERROR(rValidator_->appendPlan(lValidator_->root()));
  auto node = static_cast<SingleInputNode*>(rValidator_->tail());
  if (node->inputVar().empty()) {
    // If the input variable was not set, set it dynamically.
    node->setInputVar(lValidator_->root()->outputVar());
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
