%option c++
%option yyclass="WKTScanner"
%option nodefault noyywrap
%option never-interactive
%option yylineno
%option case-insensitive
%option prefix="wkt"

%{
#include "common/geo/io/wkt/WKTScanner.h"
#include "WKTParser.hpp"
#include <stdlib.h>

#define YY_USER_ACTION                  \
    yylloc->step();                     \
    yylloc->columns(yyleng);

%}

%%

 /* WKT shape type prefix */
"POINT"                     { return TokenType::KW_POINT; }
"LINESTRING"                { return TokenType::KW_LINESTRING; }
"POLYGON"                   { return TokenType::KW_POLYGON; }

","                         { return TokenType::COMMA; }
"("                         { return TokenType::L_PAREN; }
")"                         { return TokenType::R_PAREN; }

-?(([0-9]+\.?)|([0-9]*\.?[0-9]+)([eE][-+]?[0-9]+)?) {
    yylval->doubleVal = atof(yytext);
    return TokenType::DOUBLE;
}

[ \t\n\r]                      {}

.                           {
                                /**
                                 * Any other unmatched byte sequences will get us here,
                                 * including the non-ascii ones, which are negative
                                 * in terms of type of `signed char'. At the same time, because
                                 * Bison translates all negative tokens to EOF(i.e. YY_NULL),
                                 * so we have to cast illegal characters to type of `unsinged char'
                                 * This will make Bison receive an unknown token, which leads to
                                 * a syntax error.
                                 *
                                 * Please note that it is not Flex but Bison to regard illegal
                                 * characters as errors, in such case.
                                 */
                                return static_cast<unsigned char>(yytext[0]);

                                /**
                                 * Alternatively, we could report illegal characters by
                                 * throwing a `syntax_error' exception.
                                 * In such a way, we could distinguish illegal characters
                                 * from normal syntax errors, but at cost of poor performance
                                 * incurred by the expensive exception handling.
                                 */
                                // throw WKTParser::syntax_error(*yylloc, "char illegal");
                            }

%%
