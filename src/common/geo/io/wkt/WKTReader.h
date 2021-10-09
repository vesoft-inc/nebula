/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/geo/io/Geometry.h"
#include "common/geo/io/wkt/WKTParser.hpp"
#include "common/geo/io/wkt/WKTScanner.h"

namespace nebula {

class WKTReader {
 public:
  WKTReader() : parser_(scanner_, error_, &geom_) {
    // Callback invoked by WKTScanner
    auto readBuffer = [this](char *buf, int maxSize) -> int {
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
      return n;  // Number of bytes we actually filled in `buf'
    };
    scanner_.setReadBuffer(std::move(readBuffer));
  }

  ~WKTReader() {
    if (geom_ != nullptr) delete geom_;
  }

  StatusOr<Geometry> read(std::string wkt) {
    // Since WKTScanner needs a writable buffer, we have to copy the query string
    buffer_ = std::move(wkt);
    pos_ = &buffer_[0];
    end_ = pos_ + buffer_.size();

    scanner_.setWKT(&buffer_);
    if (parser_.parse() != 0) {
      pos_ = nullptr;
      end_ = nullptr;
      // To flush the internal buffer to recover from a failure
      scanner_.flushBuffer();
      if (geom_ != nullptr) {
        delete geom_;
        geom_ = nullptr;
      }
      scanner_.setWKT(nullptr);
      return Status::SyntaxError(error_);
    }

    if (geom_ == nullptr) {
      return Status::StatementEmpty();  // WKTEmpty()
    }
    auto geom = geom_;
    geom_ = nullptr;
    scanner_.setWKT(nullptr);
    auto tmp = std::move(*geom);
    delete geom;
    return tmp;
  }

 private:
  std::string buffer_;
  const char *pos_{nullptr};
  const char *end_{nullptr};
  nebula::WKTScanner scanner_;
  nebula::WKTParser parser_;
  std::string error_;
  Geometry *geom_{nullptr};
};

}  // namespace nebula
