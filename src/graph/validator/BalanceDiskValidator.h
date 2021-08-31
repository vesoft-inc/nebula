/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_VALIDATOR_BALANCEDISKVALIDATOR_H_
#define GRAPH_VALIDATOR_BALANCEDISKVALIDATOR_H_

#include "graph/validator/Validator.h"
#include "parser/AdminSentences.h"

namespace nebula {
namespace graph {

class BalanceDiskValidator final : public Validator {
 public:
  BalanceDiskValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override { return Status::OK(); }

  Status toPlan() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_BALANCEDISKVALIDATOR_H_
