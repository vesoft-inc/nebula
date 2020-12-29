/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_GQLPARSER_H_
#define PARSER_GQLPARSER_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "parser/GraphParser.hpp"
#include "parser/GraphScanner.h"

namespace nebula {

class GQLParser {
public:
    explicit GQLParser(nebula::graph::QueryContext *qctx = nullptr)
        : parser_(scanner_, error_, &sentences_, qctx) {
        // Callback invoked by GraphScanner
        auto readBuffer = [this] (char *buf, int maxSize) -> int {
            // Reach the end
            if (pos_ >= end_) {
                pos_ = nullptr;
                end_ = nullptr;
                return 0;
            }
            int left = end_ - pos_;
            auto n = maxSize > left ? left : maxSize;
            ::memcpy(buf, pos_, n);
            pos_ += n;
            return n;   // Number of bytes we actually filled in `buf'
        };
        scanner_.setReadBuffer(std::move(readBuffer));
    }

    ~GQLParser() {
        if (sentences_ != nullptr) delete sentences_;
    }

    StatusOr<std::unique_ptr<Sentence>> parse(std::string query) {
        // Since GraphScanner needs a writable buffer, we have to copy the query string
        buffer_ = std::move(query);
        pos_ = &buffer_[0];
        end_ = pos_ + buffer_.size();

        scanner_.setQuery(&buffer_);
        if (parser_.parse() != 0) {
            pos_ = nullptr;
            end_ = nullptr;
            // To flush the internal buffer to recover from a failure
            scanner_.flushBuffer();
            if (sentences_ != nullptr) {
                delete sentences_;
                sentences_ = nullptr;
            }
            scanner_.setQuery(nullptr);
            return Status::SyntaxError(error_);
        }

        if (sentences_ == nullptr) {
            return Status::StatementEmpty();
        }
        auto *sentences = sentences_;
        sentences_ = nullptr;
        scanner_.setQuery(nullptr);
        return std::unique_ptr<Sentence>(sentences);
    }

private:
    std::string                     buffer_;
    const char                     *pos_{nullptr};
    const char                     *end_{nullptr};
    nebula::GraphScanner            scanner_;
    nebula::GraphParser             parser_;
    std::string                     error_;
    Sentence                       *sentences_{nullptr};
};

}   // namespace nebula

#endif  // PARSER_GQLPARSER_H_
