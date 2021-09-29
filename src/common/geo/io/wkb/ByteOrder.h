/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <boost/endian/conversion.hpp>

#include "common/base/Base.h"

namespace nebula {

enum class ByteOrder : uint8_t {
  BigEndian = 0,
  LittleEndian = 1,
};

struct ByteOrderData {
  static ByteOrder getMachineByteOrder() {
    static int endianCheck = 1;
    return static_cast<ByteOrder>(
        *(reinterpret_cast<char *>(&endianCheck)));  // 0 for BigEndian, 1 for LittleEndian
  }

  static uint32_t getUint32(const uint8_t *buf, ByteOrder byteOrder) {
    if (byteOrder == ByteOrder::BigEndian) {
      return boost::endian::load_big_u32(buf);
    } else {
      DCHECK(byteOrder == ByteOrder::LittleEndian);
      return boost::endian::load_little_u32(buf);
    }
  }

  static uint64_t getUint64(const uint8_t *buf, ByteOrder byteOrder) {
    if (byteOrder == ByteOrder::BigEndian) {
      return boost::endian::load_big_u64(buf);
    } else {
      DCHECK(byteOrder == ByteOrder::LittleEndian);
      return boost::endian::load_little_u64(buf);
    }
  }

  static double getDouble(const uint8_t *buf, ByteOrder byteOrder) {
    uint64_t v = getUint64(buf, byteOrder);
    double ret;
    std::memcpy(&ret, &v, sizeof(double));
    return ret;
  }
};

}  // namespace nebula
