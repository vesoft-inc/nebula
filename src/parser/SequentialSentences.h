/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_SEQUENTIALSENTENCES_H_
#define PARSER_SEQUENTIALSENTENCES_H_

#include "common/base/Base.h"
#include "parser/AdminSentences.h"
#include "parser/MaintainSentences.h"
#include "parser/MatchSentence.h"
#include "parser/MutateSentences.h"
#include "parser/ProcessControlSentences.h"
#include "parser/Sentence.h"
#include "parser/TraverseSentences.h"
#include "parser/UserSentences.h"

namespace nebula {

class SequentialSentences final : public Sentence {
 public:
  explicit SequentialSentences(Sentence *sentence) {
    kind_ = Kind::kSequential;
    sentences_.emplace_back(sentence);
  }

  void addSentence(Sentence *sentence) { sentences_.emplace_back(sentence); }

  auto sentences() const {
    std::vector<Sentence *> result;
    result.resize(sentences_.size());
    auto get = [](auto &ptr) { return ptr.get(); };
    std::transform(sentences_.begin(), sentences_.end(), result.begin(), get);
    return result;
  }

  std::string toString() const override;

 private:
  std::vector<std::unique_ptr<Sentence>> sentences_;
};

}  // namespace nebula

#endif  // PARSER_SEQUENTIALSENTENCES_H_
