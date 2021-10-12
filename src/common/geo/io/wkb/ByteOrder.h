/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <boost/endian/conversion.hpp>

#include "common/base/Base.h"

namespace nebula {
namespace geo {

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

  static void putUint32(uint8_t *buf, ByteOrder byteOrder, uint32_t v) {
    if (byteOrder == ByteOrder::BigEndian) {
      boost::endian::store_big_u32(buf, v);
    } else {
      DCHECK(byteOrder == ByteOrder::LittleEndian);
      boost::endian::store_little_u32(buf, v);
    }
  }

  static void putUint64(uint8_t *buf, ByteOrder byteOrder, uint64_t v) {
    if (byteOrder == ByteOrder::BigEndian) {
      boost::endian::store_big_u64(buf, v);
    } else {
      DCHECK(byteOrder == ByteOrder::LittleEndian);
      boost::endian::store_little_u64(buf, v);
    }
  }

  static void putDouble(uint8_t *buf, ByteOrder byteOrder, double v) {
    const char *c = reinterpret_cast<const char *>(&v);
    uint64_t v2 = *reinterpret_cast<const uint64_t *>(c);
    putUint64(buf, byteOrder, v2);
  }
};

}  // namespace geo
}  // namespace nebula
