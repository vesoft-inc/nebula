/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_STATEMENT_H_
#define PARSER_STATEMENT_H_

#include "parser/MaintainSentences.h"
#include "parser/TraverseSentences.h"
#include "parser/MutateSentences.h"

namespace vesoft {

class Statement final {
public:
    explicit Statement(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    void addSentence(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<Sentence>>        sentences_;
};


}   // namespace vesoft

#endif  // PARSER_STATEMENT_H_
