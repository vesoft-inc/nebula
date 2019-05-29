/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_MD5UTILS_H
#define COMMON_BASE_MD5UTILS_H

#include "proxygen/lib/utils/CryptUtil.h"

namespace nebula {
class MD5Utils final {
public:
static std::string md5Encode(const std::string &str) {
    return proxygen::md5Encode(folly::ByteRange(
            reinterpret_cast<const unsigned char*>(str.c_str()),
            str.length()));
}
};
}
#endif  // COMMON_BASE_MD5UTILS_H
