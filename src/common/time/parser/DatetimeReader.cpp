/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/time/parser/DatetimeReader.h"

namespace nebula {
namespace time {

DatetimeReader::DatetimeReader() : parser_(scanner_, error_, &dt_) {
  // Callback invoked by DatetimeScanner
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

StatusOr<DateTime> DatetimeReader::read(std::string input) {
  // Since DatetimeScanner needs a writable buffer, we have to copy the query string
  buffer_ = std::move(input);
  pos_ = &buffer_[0];
  end_ = pos_ + buffer_.size();

  scanner_.setInput(&buffer_);
  if (parser_.parse() != 0) {
    pos_ = nullptr;
    end_ = nullptr;
    // To flush the internal buffer to recover from a failure
    scanner_.flushBuffer();
    if (dt_ != nullptr) {
      delete dt_;
      dt_ = nullptr;
    }
    scanner_.setInput(nullptr);
    return Status::SyntaxError(error_);
  }

  if (dt_ == nullptr) {
    return Status::StatementEmpty();  // empty
  }
  auto dt = dt_;
  dt_ = nullptr;
  scanner_.setInput(nullptr);
  auto tmp = std::move(*dt);
  delete dt;
  return tmp;
}

}  // namespace time
}  // namespace nebula
