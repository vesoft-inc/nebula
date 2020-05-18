/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/encryption/Base64.h"

namespace nebula {
namespace encryption {

std::string Base64::encode(const std::string &toEncodeStr) {
    return proxygen::base64Encode(folly::ByteRange(
        reinterpret_cast<const unsigned char*>(toEncodeStr.c_str()),
        toEncodeStr.length()));
}

}  // namespace encryption
}  // namespace nebula
