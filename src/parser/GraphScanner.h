/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_GRAPHSCANNER_H_
#define PARSER_GRAPHSCANNER_H_

#include "base/Base.h"

// Only include FlexLexer.h if it hasn't been already included
#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

// Override the interface for yylex since we namespaced it
#undef YY_DECL
#define YY_DECL int nebula::GraphScanner::yylex()

#include "GraphParser.hpp"

namespace nebula {

class GraphScanner : public yyFlexLexer {
public:
    int yylex(nebula::GraphParser::semantic_type * lval,
              nebula::GraphParser::location_type *loc) {
        yylval = lval;
        yylloc = loc;
        return yylex();
    }


private:
    friend class Scanner_Basic_Test;
    int yylex();

    nebula::GraphParser::semantic_type * yylval{nullptr};
    nebula::GraphParser::location_type * yylloc{nullptr};
};

}   // namespace nebula

#endif  // PARSER_GRAPHSCANNER_H_
