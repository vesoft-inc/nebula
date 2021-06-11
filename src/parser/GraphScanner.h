/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_GRAPHSCANNER_H_
#define PARSER_GRAPHSCANNER_H_

#include "common/base/Base.h"

// Only include FlexLexer.h if it hasn't been already included
#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

// Override the interface for yylex since we namespaced it
#undef YY_DECL
#define YY_DECL int nebula::GraphScanner::yylex()

#include "parser/GraphParser.hpp"

namespace nebula {

class GraphScanner : public yyFlexLexer {
public:
    int yylex(nebula::GraphParser::semantic_type * lval,
              nebula::GraphParser::location_type *loc) {
        yylval = lval;
        yylloc = loc;
        return yylex();
    }

public:
    // Called by GQLParser to set the `readBuffer' callback, which would be invoked
    // by LexerInput to fill the stream buffer.
    void setReadBuffer(std::function<int(char*, int)> readBuffer) {
        readBuffer_ = readBuffer;
    }

    // Manually invoked by GQLParser to recover from a failure state.
    // This makes the scanner reentrant.
    void flushBuffer() {
        yy_flush_buffer(yy_buffer_stack ? yy_buffer_stack[yy_buffer_stack_top] : nullptr);
    }

    void setQuery(std::string *query) {
        query_ = query;
    }

    std::string* query() {
        return query_;
    }

    void setUnaryMinus(bool v) {
        hasUnaryMinus_ = v;
    }

    bool hasUnaryMinus() const {
        return hasUnaryMinus_;
    }

    void setIsIntMin(bool v) {
        isIntMin_ = v;
    }

    bool isIntMin() const {
        return isIntMin_;
    }

protected:
    // Called when YY_INPUT is invoked
    int LexerInput(char *buf, int maxSize) override {
        return readBuffer_(buf, maxSize);
    }

    void makeSpaceForString(size_t len) {
        constexpr auto defaultSize = 256UL;
        if (sbuf_ == nullptr) {
            sbufSize_ = defaultSize > len ? defaultSize : len;
            sbuf_ = std::make_unique<char[]>(sbufSize_);
            return;
        }

        if (sbufSize_ - sbufPos_ >= len) {
            return;
        }

        auto newSize = sbufSize_ * 2 + len;
        auto newBuffer = std::make_unique<char[]>(newSize);
        ::memcpy(newBuffer.get(), sbuf_.get(), sbufPos_);
        sbuf_ = std::move(newBuffer);
        sbufSize_ = newSize;
    }

    char* sbuf() {
        return sbuf_.get();
    }

    using TokenType = nebula::GraphParser::token;
    auto parseDecimal() {
        try {
            folly::StringPiece text(yytext, yyleng);
            uint64_t val = folly::to<uint64_t>(text);
            if (val > MAX_ABS_INTEGER) {
                throw GraphParser::syntax_error(*yylloc, "Out of range:");
            }
            if (val == MAX_ABS_INTEGER && !hasUnaryMinus()) {
                throw GraphParser::syntax_error(*yylloc, "Out of range:");
            }
            if (val == MAX_ABS_INTEGER && hasUnaryMinus()) {
                setIsIntMin(true);
            }
            yylval->intval = val;
        } catch (...) {
            throw GraphParser::syntax_error(*yylloc, "Out of range:");
        }
        return TokenType::INTEGER;
    }

    auto parseDouble() const {
        try {
            folly::StringPiece text(yytext, yyleng);
            yylval->doubleval = folly::to<double>(text);
        } catch (...) {
            throw GraphParser::syntax_error(*yylloc, "Out of range:");
        }
        return TokenType::DOUBLE;
    }

    auto parseHex() {
        if (yyleng > 18) {
            auto i = 2;
            while (i < yyleng && yytext[i] == '0') {
                i++;
            }
            if (yyleng - i > 16) {
                throw GraphParser::syntax_error(*yylloc, "Out of range:");
            }
        }
        uint64_t val = 0;
        sscanf(yytext, "%lx", &val);
        if (val > MAX_ABS_INTEGER) {
            throw GraphParser::syntax_error(*yylloc, "Out of range:");
        }
        if (val == MAX_ABS_INTEGER && !hasUnaryMinus()) {
            throw GraphParser::syntax_error(*yylloc, "Out of range:");
        }
        if (val == MAX_ABS_INTEGER && hasUnaryMinus()) {
            setIsIntMin(true);
        }
        yylval->intval = static_cast<int64_t>(val);
        return TokenType::INTEGER;
    }

    auto parseOct() {
        if (yyleng > 22) {
            auto i = 1;
            while (i < yyleng && yytext[i] == '0') {
                i++;
            }
            if (yyleng - i > 22 ||
                    (yyleng - i == 22 && yytext[i] != '1')) {
                throw GraphParser::syntax_error(*yylloc, "Out of range:");
            }
        }
        uint64_t val = 0;
        sscanf(yytext, "%lo", &val);
        if (val > MAX_ABS_INTEGER) {
            throw GraphParser::syntax_error(*yylloc, "Out of range:");
        }
        if (val == MAX_ABS_INTEGER && !hasUnaryMinus()) {
            throw GraphParser::syntax_error(*yylloc, "Out of range:");
        }
        if (val == MAX_ABS_INTEGER && hasUnaryMinus()) {
            setIsIntMin(true);
        }
        yylval->intval = static_cast<int64_t>(val);
        return TokenType::INTEGER;
    }

private:
    friend class Scanner_Basic_Test;
    int yylex() override;

    bool                                hasUnaryMinus_{false};
    bool                                isIntMin_{false};
    nebula::GraphParser::semantic_type *yylval{nullptr};
    nebula::GraphParser::location_type *yylloc{nullptr};
    std::unique_ptr<char[]>             sbuf_{nullptr};
    size_t                              sbufSize_{0};
    size_t                              sbufPos_{0};
    std::function<int(char*, int)>      readBuffer_;
    std::string*                        query_{nullptr};
};

}   // namespace nebula

#endif  // PARSER_GRAPHSCANNER_H_
