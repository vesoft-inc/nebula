/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_TIME_PARSER_DATETIMESCANNER_H
#define COMMON_TIME_PARSER_DATETIMESCANNER_H

#include "common/base/Base.h"

// This macro must be defined before #include <FlexLexer.h> !!!
#define yyFlexLexer datetimeFlexLexer

// Only include FlexLexer.h if it hasn't been already included
#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

// Override the interface for yylex since we namespaced it
#undef YY_DECL
#define YY_DECL int nebula::time::DatetimeScanner::yylex()

#include "common/time/parser/DatetimeParser.hpp"

namespace nebula {
namespace time {

class DatetimeScanner : public yyFlexLexer {
 public:
  int yylex(nebula::time::DatetimeParser::semantic_type *lval,
            nebula::time::DatetimeParser::location_type *loc) {
    yylval = lval;
    yylloc = loc;
    return yylex();
  }

 public:
  // Called by DatetimeReader to set the `readBuffer' callback, which would be
  // invoked by LexerInput to fill the stream buffer.
  void setReadBuffer(std::function<int(char *, int)> readBuffer) {
    readBuffer_ = readBuffer;
  }

  // Manually invoked by DatetimeReader to recover from a failure state.
  // This makes the scanner reentrant.
  void flushBuffer() {
    yy_flush_buffer(yy_buffer_stack ? yy_buffer_stack[yy_buffer_stack_top] : nullptr);
  }

  void setInput(std::string *input) {
    input_ = input;
  }

  std::string *input() {
    return input_;
  }

 protected:
  // Called when YY_INPUT is invoked
  int LexerInput(char *buf, int maxSize) override {
    return readBuffer_(buf, maxSize);
  }

  using TokenType = nebula::time::DatetimeParser::token;

 private:
  // friend class Scanner_Basic_Test; TODO: add it
  int yylex() override;

  nebula::time::DatetimeParser::semantic_type *yylval{nullptr};
  nebula::time::DatetimeParser::location_type *yylloc{nullptr};
  std::function<int(char *, int)> readBuffer_;
  std::string *input_{nullptr};
};

}  // namespace time
}  // namespace nebula
#endif
