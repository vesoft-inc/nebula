/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/ExplainSentence.h"

#include <folly/String.h>  // for stringPrintf

#include <vector>  // for vector

#include "Sentence.h"                    // for Sentence, Sentence::Kind
#include "common/base/Logging.h"         // for CheckNotNull, DCHECK_NOTNULL
#include "parser/SequentialSentences.h"  // for SequentialSentences

namespace nebula {

ExplainSentence::ExplainSentence(SequentialSentences* seqSentences,
                                 bool isProfile,
                                 std::string* formatType)
    : Sentence(Kind::kExplain),
      isProfile_(isProfile),
      formatType_(!formatType ? new std::string() : formatType),
      seqSentences_(DCHECK_NOTNULL(seqSentences)) {}

std::string ExplainSentence::toString() const {
  auto seqStr = seqSentences_->toString();
  const char* keyword = isProfile_ ? "PROFILE " : "EXPLAIN ";
  auto format = formatType_->empty() ? "" : "FORMAT=\"" + formatType() + "\" ";
  bool hasMultiSentences = seqSentences_->sentences().size() > 1;
  const char* fmt = hasMultiSentences ? "%s%s{%s}" : "%s%s%s";
  return folly::stringPrintf(fmt, keyword, format.c_str(), seqStr.c_str());
}

}  // namespace nebula
