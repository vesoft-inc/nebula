/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_GQLPARSER_H_
#define PARSER_GQLPARSER_H_

#include "base/Base.h"
#include "VGraphParser.hpp"
#include "VGraphScanner.h"

namespace vesoft {

class GQLParser {
public:
    GQLParser() : parser_(scanner_, error_, &statement_) {
    }

    bool parse(const std::string &query) {
        std::istringstream is(query);
        scanner_.switch_streams(&is, nullptr);
        return parser_.parse() == 0;
    }

    auto statement() {
        std::unique_ptr<Statement> statement(statement_);
        statement_ = nullptr;
        return statement;
    }

    const std::string& error() const {
        return error_;
    }

private:
    vesoft::VGraphScanner           scanner_;
    vesoft::VGraphParser            parser_;
    std::string                     error_;
    Statement                      *statement_ = nullptr;
};

}   // namespace vesoft

#endif  // PARSER_GQLPARSER_H_
