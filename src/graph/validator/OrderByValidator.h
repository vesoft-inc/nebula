/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_ORDERBYVALIDATOR_H_
#define GRAPH_VALIDATOR_ORDERBYVALIDATOR_H_

#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {
class OrderByValidator final : public Validator {
 public:
  OrderByValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::vector<std::pair<size_t, OrderFactor::OrderType>> colOrderTypes_;
  std::string userDefinedVarName_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_VALIDATOR_ORDERBYVALIDATOR_H_
