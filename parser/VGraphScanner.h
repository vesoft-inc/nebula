/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_VGRAPHSCANNER_H_
#define PARSER_VGRAPHSCANNER_H_

// Only include FlexLexer.h if it hasn't been already included
#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

// Override the interface for yylex since we namespaced it
#undef YY_DECL
#define YY_DECL int vesoft::VGraphScanner::yylex()

#include "VGraphParser.hpp"

namespace vesoft {

class VGraphScanner : public yyFlexLexer {
public:
    int yylex(vesoft::VGraphParser::semantic_type * lval,
              vesoft::VGraphParser::location_type *loc) {
        yylval = lval;
        yylloc = loc;
        return yylex();
    }


private:
    friend class Scanner_Basic_Test;
    int yylex();

    vesoft::VGraphParser::semantic_type * yylval{nullptr};
    vesoft::VGraphParser::location_type * yylloc{nullptr};
};

}   // namespace vesoft

#endif  // PARSER_VGRAPHSCANNER_H_
