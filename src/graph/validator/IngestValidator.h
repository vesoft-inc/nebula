/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_INGESTVALIDATOR_H_
#define GRAPH_VALIDATOR_INGESTVALIDATOR_H_

#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {

class IngestValidator final : public Validator {
 public:
  IngestValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override {
    return Status::OK();
  }

  Status toPlan() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_INGESTVALIDATOR_H_
