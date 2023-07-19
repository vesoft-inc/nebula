/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_EXPLAINVALIDATOR_H
#define GRAPH_VALIDATOR_EXPLAINVALIDATOR_H

#include <memory>

#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {

class SequentialValidator;

class ExplainValidator final : public Validator {
 public:
  ExplainValidator(Sentence* sentence, QueryContext* context);

 private:
  Status validateImpl() override;

  Status toPlan() override;

  std::unique_ptr<SequentialValidator> validator_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_EXPLAINVALIDATOR_H
