/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_SEQUENTIALVALIDATOR_H_
#define GRAPH_VALIDATOR_SEQUENTIALVALIDATOR_H_

#include <memory>  // for unique_ptr
#include <vector>  // for vector

#include "common/base/Status.h"            // for Status
#include "graph/context/ast/AstContext.h"  // for AstContext
#include "graph/validator/Validator.h"     // for Validator
#include "parser/SequentialSentences.h"

namespace nebula {
class Sentence;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Sentence;

namespace graph {
class PlanNode;
class QueryContext;

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
