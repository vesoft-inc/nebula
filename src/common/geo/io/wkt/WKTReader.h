/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_GEO_IO_WKT_WKTREADER_H
#define COMMON_GEO_IO_WKT_WKTREADER_H

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Geography.h"
#include "common/geo/io/wkt/WKTParser.hpp"
#include "common/geo/io/wkt/WKTScanner.h"

namespace nebula {
namespace geo {

class WKTReader {
 public:
  WKTReader() : parser_(scanner_, error_, &geog_) {
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
    if (geog_ != nullptr) delete geog_;
  }

  StatusOr<Geography> read(std::string wkt) {
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
      if (geog_ != nullptr) {
        delete geog_;
        geog_ = nullptr;
      }
      scanner_.setWKT(nullptr);
      return Status::SyntaxError(error_);
    }

    if (geog_ == nullptr) {
      return Status::StatementEmpty();  // WKTEmpty()
    }
    auto geog = geog_;
    geog_ = nullptr;
    scanner_.setWKT(nullptr);
    auto tmp = std::move(*geog);
    delete geog;
    return tmp;
  }

 private:
  std::string buffer_;
  const char *pos_{nullptr};
  const char *end_{nullptr};
  nebula::geo::WKTScanner scanner_;
  nebula::geo::WKTParser parser_;
  std::string error_;
  Geography *geog_{nullptr};
};

}  // namespace geo
}  // namespace nebula
#endif
