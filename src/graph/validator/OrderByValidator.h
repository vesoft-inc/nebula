/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_ORDERBYVALIDATOR_H_
#define GRAPH_VALIDATOR_ORDERBYVALIDATOR_H_

#include <stddef.h>  // for size_t

#include <string>   // for string
#include <utility>  // for pair
#include <vector>   // for vector

#include "common/base/Status.h"         // for Status
#include "graph/validator/Validator.h"  // for Validator
#include "parser/TraverseSentences.h"   // for OrderFactor::OrderType

namespace nebula {
class Sentence;
namespace graph {
class QueryContext;
}  // namespace graph

class Sentence;

namespace graph {
class QueryContext;

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
