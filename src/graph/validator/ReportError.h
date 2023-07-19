/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_REPORTERROR_H_
#define GRAPH_VALIDATOR_REPORTERROR_H_

#include "common/base/Base.h"

namespace nebula {
namespace graph {

// Validator which report error directly.
class ReportError final : public Validator {
 public:
  ReportError(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
  }

 private:
  Status validateImpl() override {
    return Status::SemanticError("Not support sentence type: %ld, query: %s",
                                 static_cast<int64_t>(sentence_->kind()),
                                 sentence_->toString().c_str());
  }

  Status toPlan() override {
    return Status::SemanticError("Not support sentence type: %ld, query: %s",
                                 static_cast<int64_t>(sentence_->kind()),
                                 sentence_->toString().c_str());
  }
};
}  // namespace graph
}  // namespace nebula
#endif
