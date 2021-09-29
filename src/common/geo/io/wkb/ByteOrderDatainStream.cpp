/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/io/wkb/ByteOrderDataInStream.h"

namespace nebula {

StatusOr<uint8_t> ByteOrderDataInStream::readUint8() {
  stream->read(reinterpret_cast<char*>(buf_), 1);
  if (stream->eof()) {
    return Status::Error("Unexpected EOF parsing WKB");
  }
  return buf_[0];
}

StatusOr<uint32_t> ByteOrderDataInStream::readUint32() {
  stream->read(reinterpret_cast<char*>(buf_), 4);
  if (stream->eof()) {
    return Status::Error("Unexpected EOF parsing WKB");
  }
  return ByteOrderData::getUint32(buf_, byteOrder);
}

StatusOr<double> ByteOrderDataInStream::readDouble() {
  stream->read(reinterpret_cast<char*>(buf_), 8);
  if (stream->eof()) {
    return Status::Error("Unexpected EOF parsing WKB");
  }
  return ByteOrderData::getDouble(buf_, byteOrder);
}

}  // namespace nebula
