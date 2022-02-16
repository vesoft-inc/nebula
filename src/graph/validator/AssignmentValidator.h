/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_ASSIGNMENTVALIDATOR_H_
#define GRAPH_VALIDATOR_ASSIGNMENTVALIDATOR_H_

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/base/Status.h"         // for Status
#include "graph/validator/Validator.h"  // for Validator
#include "parser/TraverseSentences.h"   // for AssignmentSentence

namespace nebula {
class Sentence;
namespace graph {
class QueryContext;
}  // namespace graph

class Sentence;

namespace graph {
class QueryContext;

class AssignmentValidator final : public Validator {
 public:
  AssignmentValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    auto* assignSentence = static_cast<AssignmentSentence*>(sentence_);
    validator_ = makeValidator(assignSentence->sentence(), qctx_);

    if (validator_->noSpaceRequired()) {
      setNoSpaceRequired();
    }
  }

 private:
  Status validateImpl() override;

  Status toPlan() override;

 private:
  std::unique_ptr<Validator> validator_;
  std::string var_;
};
}  // namespace graph
}  // namespace nebula
#endif
