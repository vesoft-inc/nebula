/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"

// This macro must be defined before #include <FlexLexer.h> !!!
#define yyFlexLexer wktFlexLexer

// Only include FlexLexer.h if it hasn't been already included
#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

// Override the interface for yylex since we namespaced it
#undef YY_DECL
#define YY_DECL int nebula::WKTScanner::yylex()

#include "common/geo/io/wkt/WKTParser.hpp"

namespace nebula {

// TODO(jie) Try to reuse the class GraphScanner
class WKTScanner : public yyFlexLexer {
 public:
  int yylex(nebula::WKTParser::semantic_type *lval, nebula::WKTParser::location_type *loc) {
    yylval = lval;
    yylloc = loc;
    return yylex();
  }

 public:
  // Called by WKTReader to set the `readBuffer' callback, which would be
  // invoked by LexerInput to fill the stream buffer.
  void setReadBuffer(std::function<int(char *, int)> readBuffer) { readBuffer_ = readBuffer; }

  // Manually invoked by WKTReader to recover from a failure state.
  // This makes the scanner reentrant.
  void flushBuffer() {
    yy_flush_buffer(yy_buffer_stack ? yy_buffer_stack[yy_buffer_stack_top] : nullptr);
  }

  void setWKT(std::string *wkt) { wkt_ = wkt; }

  std::string *wkt() { return wkt_; }

 protected:
  // Called when YY_INPUT is invoked
  int LexerInput(char *buf, int maxSize) override { return readBuffer_(buf, maxSize); }

  using TokenType = nebula::WKTParser::token;

 private:
  // friend class Scanner_Basic_Test; TODO(jie) add it
  int yylex() override;

  nebula::WKTParser::semantic_type *yylval{nullptr};
  nebula::WKTParser::location_type *yylloc{nullptr};
  std::function<int(char *, int)> readBuffer_;
  std::string *wkt_{nullptr};
};

}  // namespace nebula
