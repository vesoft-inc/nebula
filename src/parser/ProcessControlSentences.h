/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_PROCESSCONTROLSENTENCES_H_
#define PARSER_PROCESSCONTROLSENTENCES_H_

#include "parser/Sentence.h"

namespace nebula {
class ReturnSentence final : public Sentence {
public:
    ReturnSentence(std::string *var, std::string *condition) {
        kind_ = Kind::kReturn;
        var_.reset(var);
        condition_.reset(condition);
    }

    std::string* condition() const {
        return condition_.get();
    }

    std::string* var() const {
        return var_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>    condition_;
    std::unique_ptr<std::string>    var_;
};
}  // namespace nebula
#endif  // PARSER_PROCESSCONTROLSENTENCES_H_
