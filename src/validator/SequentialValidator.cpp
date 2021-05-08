/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/SequentialValidator.h"

#include "common/base/Base.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"
#include "service/GraphFlags.h"
#include "service/PermissionCheck.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {
Status SequentialValidator::validateImpl() {
    Status status;
    if (sentence_->kind() != Sentence::Kind::kSequential) {
        return Status::SemanticError(
                "Sequential validator validates a SequentialSentences, but %ld is given.",
                static_cast<int64_t>(sentence_->kind()));
    }
    auto seqSentence = static_cast<SequentialSentences*>(sentence_);
    auto sentences = seqSentence->sentences();

    if (sentences.size() > static_cast<size_t>(FLAGS_max_allowed_statements)) {
        return Status::SemanticError("The maximum number of statements allowed has been exceeded");
    }

    DCHECK(!sentences.empty());
    auto firstSentence = getFirstSentence(sentences.front());
    switch (firstSentence->kind()) {
        case Sentence::Kind::kLimit:
        case Sentence::Kind::kOrderBy:
        case Sentence::Kind::kGroupBy:
            return Status::SyntaxError("Could not start with the statement: %s",
                                       firstSentence->toString().c_str());
        default:
            break;
    }

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
    auto pipe = static_cast<const PipedSentence *>(sentence);
    return getFirstSentence(pipe->left());
}
}  // namespace graph
}  // namespace nebula
