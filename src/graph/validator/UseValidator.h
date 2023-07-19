/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_USEVALIDATOR_H_
#define GRAPH_VALIDATOR_USEVALIDATOR_H_

#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {
class UseValidator final : public Validator {
 public:
  UseValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  /**
   * Will not check the space for use space sentence.
   */
  bool spaceChosen() override {
    return true;
  }

  Status validateImpl() override;

  Status toPlan() override;

 private:
  const std::string* spaceName_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif
