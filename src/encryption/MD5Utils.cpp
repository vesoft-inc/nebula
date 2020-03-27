/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/encryption/MD5Utils.h"

namespace nebula {
namespace encryption {
// static
std::string MD5Utils::md5Encode(const std::string &str) {
    return proxygen::md5Encode(folly::ByteRange(
        reinterpret_cast<const unsigned char*>(str.c_str()),
        str.length()));
}
}  // namespace encryption
}  // namespace nebula
