/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_EXPLAINSENTENCE_H_
#define PARSER_EXPLAINSENTENCE_H_

#include <memory>
#include <string>

#include "parser/Sentence.h"

namespace nebula {

class SequentialSentences;

class ExplainSentence final : public Sentence {
public:
    ExplainSentence(SequentialSentences* seqSentences,
                    bool isProfile = false,
                    std::string* formatType = nullptr);

    std::string toString() const override;

    bool isProfile() const {
        return isProfile_;
    }

    const std::string& formatType() const {
        return *formatType_;
    }

    SequentialSentences* seqSentences() const {
        return seqSentences_.get();
    }

private:
    bool isProfile_{false};
    std::unique_ptr<std::string> formatType_;
    std::unique_ptr<SequentialSentences> seqSentences_;
};

}   // namespace nebula

#endif   // PARSER_EXPLAINSENTENCE_H_
