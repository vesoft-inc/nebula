/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_UNWINDVALIDATOR_H_
#define GRAPH_VALIDATOR_UNWINDVALIDATOR_H_

#include "graph/planner/plan/Query.h"
#include "graph/validator/Validator.h"

namespace nebula {

class ObjectPool;

namespace graph {

class QueryContext;

class UnwindValidator final : public Validator {
 public:
  UnwindValidator(Sentence *sentence, QueryContext *context) : Validator(sentence, context) {}

  Status validateImpl() override;

  Status toPlan() override;

 private:
  Expression *unwindExpr_{nullptr};
  std::string alias_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_UNWINDVALIDATOR_H_
