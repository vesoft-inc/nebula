/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_SAMPLINGVALIDATOR_H_
#define GRAPH_VALIDATOR_SAMPLINGVALIDATOR_H_

#include "graph/planner/plan/Query.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {
class SamplingValidator final : public Validator {
 public:
  SamplingValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::vector<SamplingParams> colSamplingTypes_;
  std::string userDefinedVarName_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_VALIDATOR_SAMPLINGVALIDATOR_H_
