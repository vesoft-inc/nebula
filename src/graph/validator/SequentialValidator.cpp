/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/SequentialValidator.h"

#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/PermissionCheck.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {

// Validator of sequential sentences which combine multiple sentences, e.g. GO ...; GO ...;
// Call validator of sub-sentences.
Status SequentialValidator::validateImpl() {
  if (sentence_->kind() != Sentence::Kind::kSequential) {
    return Status::SemanticError(
        "Sequential validator validates a SequentialSentences, but %ld is "
        "given.",
        static_cast<int64_t>(sentence_->kind()));
  }
  auto seqSentence = static_cast<SequentialSentences*>(sentence_);
  auto sentences = seqSentence->sentences();

  if (sentences.size() > static_cast<size_t>(FLAGS_max_statements) + 1) {
    return Status::SemanticError("The maximum number of statements has been exceeded");
  }
  if (sentences.size() > static_cast<size_t>(FLAGS_max_allowed_statements)) {
    return Status::SemanticError("The maximum number of statements allowed has been exceeded");
  }

  DCHECK(!sentences.empty());

  seqAstCtx_->startNode = StartNode::make(seqAstCtx_->qctx);
  for (auto* sentence : sentences) {
    auto validator = makeValidator(sentence, qctx_);
    NG_RETURN_IF_ERROR(validator->validate());
    seqAstCtx_->validators.emplace_back(std::move(validator));
  }

  return Status::OK();
}

// Get first sentence in nested sentence.
const Sentence* SequentialValidator::getFirstSentence(const Sentence* sentence) const {
  if (sentence->kind() != Sentence::Kind::kPipe) {
    return sentence;
  }
  auto pipe = static_cast<const PipedSentence*>(sentence);
  return getFirstSentence(pipe->left());
}

}  // namespace graph
}  // namespace nebula
