/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_GQLPARSER_H_
#define PARSER_GQLPARSER_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "GraphParser.hpp"
#include "GraphScanner.h"

namespace nebula {

class GQLParser {
public:
    GQLParser() : parser_(scanner_, error_, &compound_) {
    }

    ~GQLParser() {
        if (compound_ != nullptr) {
            delete compound_;
        }
    }

    StatusOr<std::unique_ptr<CompoundSentence>> parse(const std::string &query) {
        std::istringstream is(query);
        scanner_.switch_streams(&is, nullptr);
        auto ok = parser_.parse() == 0;
        if (!ok) {
            return Status::SyntaxError(error_);
        }
        auto *compound = compound_;
        compound_ = nullptr;
        return compound;
    }

private:
    nebula::GraphScanner            scanner_;
    nebula::GraphParser             parser_;
    std::string                     error_;
    CompoundSentence               *compound_ = nullptr;
};

}   // namespace nebula

#endif  // PARSER_GQLPARSER_H_
