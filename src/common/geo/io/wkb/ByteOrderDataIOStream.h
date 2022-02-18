/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_GEO_IO_WKB_BYTEORDERDATAIOSTREAM_H
#define COMMON_GEO_IO_WKB_BYTEORDERDATAIOSTREAM_H

#include <sstream>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/geo/io/wkb/ByteOrder.h"

namespace nebula {
namespace geo {

class ByteOrderDataInStream {
 public:
  ByteOrderDataInStream() = default;
  explicit ByteOrderDataInStream(const std::string& s) : stream_(s) {}

  ~ByteOrderDataInStream() = default;

  std::string str() const {
    return stream_.str();
  }

  void setInput(const std::string& s) {
    stream_.str(s);
  }

  void setByteOrder(ByteOrder order) {
    byteOrder_ = order;
  }

  StatusOr<uint8_t> readUint8();

  StatusOr<uint32_t> readUint32();

  StatusOr<double> readDouble();

 private:
  ByteOrder byteOrder_;
  std::istringstream stream_;
  unsigned char buf_[8];
};

class ByteOrderDataOutStream {
 public:
  ByteOrderDataOutStream() = default;

  ~ByteOrderDataOutStream() = default;

  std::string str() const {
    return stream_.str();
  }

  void setByteOrder(ByteOrder order) {
    byteOrder_ = order;
  }

  void writeUint8(uint8_t v);

  void writeUint32(uint32_t v);

  void writeDouble(double v);

 private:
  ByteOrder byteOrder_;
  std::ostringstream stream_;
  unsigned char buf_[8];
};

}  // namespace geo
}  // namespace nebula
#endif
