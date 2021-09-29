/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <sstream>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/geo/io/wkb/ByteOrder.h"

namespace nebula {

class ByteOrderDataInStream {
 public:
  explicit ByteOrderDataInStream(std::string& s) {
    std::istringstream is(s);
    stream_ = &is;
  }

  explicit ByteOrderDataInStream(std::istream* is) : stream_(is) {}

  ~ByteOrderDataInStream() = default;

  void setByteOrder(ByteOrder order) { byteOrder_ = order; }

  StatusOr<uint8_t> readUint8();

  StatusOr<uint32_t> readUint32();

  StatusOr<double> readDouble();

 private:
  ByteOrder byteOrder_;
  std::istream* stream_;
  unsigned char buf_[8];
};

}  // namespace nebula
