/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

namespace nebula {

enum class ByteOrder : uint8_t {
  BigEndian = 0,
  LittleEndian = 1,
};

ByteOrder getMachineByteOrder() {
  static int endianCheck = 1;
  return static_cast<ByteOrder>(
      *(reinterpret_cast<char*>(&endianCheck)));  // 0 for BigEndian, 1 for LittleEndian
}

}  // namespace nebula
