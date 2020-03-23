/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_ENCRYPTION_BASE64_H_
#define COMMON_ENCRYPTION_BASE64_H_

#include "base/Base.h"

namespace nebula {
namespace encryption {

class Base64 final {
public:
    Base64() = delete;

    // Encode three bytes into four bytes, pad "=" if less than three bytes
    // Each 6 bit value is used as an array index to take the display character in encodeTab_
    // 6 bit value -> array index -> base64 characters
    static std::string encode(const std::string &toEncodeStr);

    // Decode base64 encoded string
    // When decoding, processes four bytes at a time
    // Get the value of the corresponding of the character in the decodeTab_,
    // the value is the array index in encodeTab_
    // base64 characters ->array index -> 6 bit value
    static std::string decode(const std::string &toDecodeStr);

private:
    // Base64 dictionary
    static const char encodeTab_[];

    // Decode table
    static const int decodeTab_[];
};
}  // namespace encryption
}  // namespace nebula
#endif  // COMMON_ENCRYPTION_BASE64_H_
