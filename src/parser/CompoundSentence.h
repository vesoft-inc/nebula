/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_COMPOUNDSENTENCE_H_
#define PARSER_COMPOUNDSENTENCE_H_

#include "base/Base.h"
#include "parser/MaintainSentences.h"
#include "parser/TraverseSentences.h"
#include "parser/MutateSentences.h"

namespace nebula {

namespace graph {
class CompoundExecutor;
}

class CompoundSentence final {
public:
    explicit CompoundSentence(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    void addSentence(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    std::string toString() const;

private:
    friend class nebula::graph::CompoundExecutor;
    std::vector<std::unique_ptr<Sentence>>        sentences_;
};


}   // namespace nebula

#endif  // PARSER_COMPOUNDSENTENCE_H_
