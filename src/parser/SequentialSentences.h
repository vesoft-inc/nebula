/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_SEQUENTIALSENTENCES_H_
#define PARSER_SEQUENTIALSENTENCES_H_

#include "base/Base.h"
#include "parser/MaintainSentences.h"
#include "parser/TraverseSentences.h"
#include "parser/MutateSentences.h"
#include "parser/AdminSentences.h"
#include "parser/UserSentences.h"
#include "parser/ProcessControlSentences.h"

namespace nebula {

namespace graph {
class SequentialExecutor;
}

class SequentialSentences final {
public:
    explicit SequentialSentences(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    void addSentence(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    auto sentences() const {
        std::vector<Sentence*> result;
        result.resize(sentences_.size());
        auto get = [] (auto &ptr) { return ptr.get(); };
        std::transform(sentences_.begin(), sentences_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    friend class nebula::graph::SequentialExecutor;
    std::vector<std::unique_ptr<Sentence>>      sentences_;
};


}   // namespace nebula

#endif  // PARSER_SEQUENTIALSENTENCES_H_
