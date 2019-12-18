/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "datamanlite/DataCommon.h"

namespace nebula {
namespace dataman {
namespace codec {

int32_t decodeVarint(const int8_t* begin, size_t len, uint64_t& val) {
    const int8_t* end = begin + len;
    const int8_t* p = begin;
    int shift = 0;
    while (p != end && *p < 0) {
        val |= static_cast<uint64_t>(*p++ & 0x7f) << shift;
        shift += 7;
    }
    if (p == end) {
        return -1;
    }
    val |= static_cast<uint64_t>(*p++) << shift;
    return p - begin;
}

size_t encodeVarint(uint64_t val, uint8_t* buf) {
    uint8_t* p = buf;
    while (val >= 128) {
        *p++ = 0x80 | (val & 0x7f);
        val >>= 7;
    }
    *p++ = uint8_t(val);
    return size_t(p - buf);
}

}  // namespace codec
}  // namespace dataman
}  // namespace nebula

