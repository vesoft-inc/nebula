/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

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
      return ((uint32_t)(buf[0] & 0xff) << 24) | ((uint32_t)(buf[1] & 0xff) << 16) |
             ((uint32_t)(buf[2] & 0xff) << 8) | ((uint32_t)(buf[3] & 0xff));
    } else {
      DCHECK(byteOrder == ByteOrder::LittleEndian);
      return ((uint32_t)(buf[3] & 0xff) << 24) | ((uint32_t)(buf[2] & 0xff) << 16) |
             ((uint32_t)(buf[1] & 0xff) << 8) | ((uint32_t)(buf[0] & 0xff));
    }
  }

  static uint64_t getUint64(const uint8_t *buf, ByteOrder byteOrder) {
    if (byteOrder == ByteOrder::BigEndian) {
      return (uint64_t)(buf[0]) << 56 | (uint64_t)(buf[1] & 0xff) << 48 |
             (uint64_t)(buf[2] & 0xff) << 40 | (uint64_t)(buf[3] & 0xff) << 32 |
             (uint64_t)(buf[4] & 0xff) << 24 | (uint64_t)(buf[5] & 0xff) << 16 |
             (uint64_t)(buf[6] & 0xff) << 8 | (uint64_t)(buf[7] & 0xff);
    } else {
      DCHECK(byteOrder == ByteOrder::LittleEndian);
      return (uint64_t)(buf[7]) << 56 | (uint64_t)(buf[6] & 0xff) << 48 |
             (uint64_t)(buf[5] & 0xff) << 40 | (uint64_t)(buf[4] & 0xff) << 32 |
             (uint64_t)(buf[3] & 0xff) << 24 | (uint64_t)(buf[2] & 0xff) << 16 |
             (uint64_t)(buf[1] & 0xff) << 8 | (uint64_t)(buf[0] & 0xff);
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
