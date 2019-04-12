/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_BASE_STRINGSWITCH_H_
#define COMMON_BASE_STRINGSWITCH_H_

#include "base/Base.h"


namespace nebula {

using hash_t = size_t;

constexpr hash_t prime = 0x100000001B3ull;
constexpr hash_t basis = 0xCBF29CE484222325ull;

hash_t StringSwitch(const char* str) {
    hash_t ret = basis;

    while (*str) {
        ret ^= *str;
        ret *= prime;
        str++;
    }
    return ret;
}

constexpr hash_t StringSwitchCase(const char* str, hash_t lastValue = basis) {
    return *str ? StringSwitchCase(str + 1, (*str ^ lastValue) * prime) : lastValue;
}

constexpr hash_t operator "" _hash(const char* p, size_t) {
    return StringSwitchCase(p);
}

}  // namespace nebula

#endif  // COMMON_BASE_STRINGSWITCH_H_
