%option c++
%option yyclass="DatetimeScanner"
%option nodefault noyywrap
%option never-interactive
%option yylineno
%option case-sensitive
%option prefix="datetime"

%{
#include "common/time/parser/DatetimeScanner.h"
#include "DatetimeParser.hpp"
#include <stdlib.h>
#include <string>

#define YY_USER_ACTION                  \
    yylloc->step();                     \
    yylloc->columns(yyleng);

%}

DEC                         ([0-9])
L_BRACKET                   "["
R_BRACKET                   "]"

%%

 /* date time shape type prefix */
"T"                     { return TokenType::KW_TIME_ID; }
":"                     { return TokenType::TIME_DELIMITER; }
" "                     { return TokenType::SPACE; }
"+"                     { return TokenType::POSITIVE; }
"-"                     { return TokenType::NEGATIVE; }

"DATETIME__"            { return TokenType::KW_DATETIME; }
"DATE__"                { return TokenType::KW_DATE; }
"TIME__"                { return TokenType::KW_TIME; }


{DEC}+                  {
                          try {
                            folly::StringPiece text(yytext, yyleng);
                            uint64_t val = folly::to<uint64_t>(text);
                            yylval->intVal = val;
                          } catch (...) {
                            throw DatetimeParser::syntax_error(*yylloc, "Invalid integer:");
                          }
                          return TokenType::INTEGER;
                        }

{DEC}+\.{DEC}+          {
                          try {
                            folly::StringPiece text(yytext, yyleng);
                            yylval->doubleVal = folly::to<double>(text);
                          } catch (...) {
                            throw DatetimeParser::syntax_error(*yylloc, "Invalid double value:");
                          }
                          return TokenType::DOUBLE;
                        }

\[[^\]]+\]              {
                          std::string *str = new std::string(yytext + 1, yyleng - 2);
                          yylval->strVal = str;
                          return TokenType::TIME_ZONE_NAME;
                        }

\n|.                    {
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
                          // throw DatetimeParser::syntax_error(*yylloc, "char illegal");
                          }

%%
