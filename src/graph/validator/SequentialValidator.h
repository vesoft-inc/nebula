/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_SEQUENTIALVALIDATOR_H_
#define GRAPH_VALIDATOR_SEQUENTIALVALIDATOR_H_

#include "graph/validator/Validator.h"
#include "parser/SequentialSentences.h"

namespace nebula {
namespace graph {

struct SequentialAstContext final : AstContext {
  std::vector<std::unique_ptr<Validator>> validators;
  PlanNode* startNode;
};

/**
 * A SequentialValidator is the entrance of validators.
 */
class SequentialValidator final : public Validator {
 public:
  SequentialValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {
    setNoSpaceRequired();
    seqAstCtx_ = getContext<SequentialAstContext>();
  }

  Status validateImpl() override;

  AstContext* getAstContext() override {
    return seqAstCtx_.get();
  }

 private:
  /**
   * Will not check the space from the beginning of a query.
   */
  bool spaceChosen() override {
    return true;
  }

  const Sentence* getFirstSentence(const Sentence* sentence) const;

  std::unique_ptr<SequentialAstContext> seqAstCtx_;
};
}  // namespace graph
}  // namespace nebula
#endif
