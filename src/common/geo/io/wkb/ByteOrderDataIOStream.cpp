/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/geo/io/wkb/ByteOrderDataIOStream.h"

namespace nebula {
namespace geo {

StatusOr<uint8_t> ByteOrderDataInStream::readUint8() {
  stream_.read(reinterpret_cast<char*>(buf_), 1);
  if (stream_.eof()) {
    return Status::Error("Unexpected EOF when reading uint8_t");
  }
  return buf_[0];
}

StatusOr<uint32_t> ByteOrderDataInStream::readUint32() {
  stream_.read(reinterpret_cast<char*>(buf_), 4);
  if (stream_.eof()) {
    return Status::Error("Unexpected EOF when reading uint32_t");
  }
  return ByteOrderData::getUint32(buf_, byteOrder_);
}

StatusOr<double> ByteOrderDataInStream::readDouble() {
  stream_.read(reinterpret_cast<char*>(buf_), 8);
  if (stream_.eof()) {
    return Status::Error("Unexpected EOF when reading double");
  }
  return ByteOrderData::getDouble(buf_, byteOrder_);
}

void ByteOrderDataOutStream::writeUint8(uint8_t v) {
  buf_[0] = v;
  stream_.write(reinterpret_cast<char*>(buf_), 1);
}

void ByteOrderDataOutStream::writeUint32(uint32_t v) {
  ByteOrderData::putUint32(buf_, byteOrder_, v);
  stream_.write(reinterpret_cast<char*>(buf_), 4);
}

void ByteOrderDataOutStream::writeDouble(double v) {
  ByteOrderData::putDouble(buf_, byteOrder_, v);
  stream_.write(reinterpret_cast<char*>(buf_), 8);
}

}  // namespace geo
}  // namespace nebula
