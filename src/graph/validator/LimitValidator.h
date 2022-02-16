/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_LIMITVALIDATOR_H_
#define GRAPH_VALIDATOR_LIMITVALIDATOR_H_

#include <stdint.h>  // for int64_t

#include "common/base/Status.h"         // for Status
#include "graph/validator/Validator.h"  // for Validator

namespace nebula {
class Sentence;
namespace graph {
class QueryContext;
}  // namespace graph

class Sentence;

namespace graph {
class QueryContext;

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
