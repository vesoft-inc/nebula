/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/SequentialValidator.h"

#include <gflags/gflags_declare.h>  // for DECLARE_uint32
#include <stddef.h>                 // for size_t
#include <stdint.h>                 // for int64_t

#include <utility>  // for move

#include "common/base/Logging.h"         // for COMPACT_GOOGLE_LOG_FATAL
#include "graph/planner/plan/Logic.h"    // for StartNode
#include "parser/Sentence.h"             // for Sentence, Sentence::Kind
#include "parser/SequentialSentences.h"  // for SequentialSentences
#include "parser/TraverseSentences.h"    // for PipedSentence

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {
Status SequentialValidator::validateImpl() {
  Status status;
  if (sentence_->kind() != Sentence::Kind::kSequential) {
    return Status::SemanticError(
        "Sequential validator validates a SequentialSentences, but %ld is "
        "given.",
        static_cast<int64_t>(sentence_->kind()));
  }
  auto seqSentence = static_cast<SequentialSentences*>(sentence_);
  auto sentences = seqSentence->sentences();

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

const Sentence* SequentialValidator::getFirstSentence(const Sentence* sentence) const {
  if (sentence->kind() != Sentence::Kind::kPipe) {
    return sentence;
  }
  auto pipe = static_cast<const PipedSentence*>(sentence);
  return getFirstSentence(pipe->left());
}

}  // namespace graph
}  // namespace nebula
