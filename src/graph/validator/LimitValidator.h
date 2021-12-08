/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_LIMITVALIDATOR_H_
#define GRAPH_VALIDATOR_LIMITVALIDATOR_H_

#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {
class LimitValidator final : public Validator {
 public:
  LimitValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  int64_t offset_{-1};
  int64_t count_{-1};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_VALIDATOR_LIMITVALIDATOR_H_
