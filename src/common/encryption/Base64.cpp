/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/encryption/Base64.h"
#include <cctype>

namespace nebula {
namespace encryption {

const char Base64::encodeTab_[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const int Base64::decodeTab_[] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, 62,    // '+'
    -1, -1, -1, 63,    // '/'
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61,     // '0'-'9'
    -1, -1, -1, -1, -1, -1, -1,
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25,     // 'A'-'Z'
    -1, -1, -1, -1, -1, -1,
    26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
    36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
    46, 47, 48, 49, 50, 51,     // 'a'-'z'
};

std::string Base64::encode(const std::string &toEncodeStr) {
    auto bytes = toEncodeStr.length();
    std::string encodeRet;

    if (toEncodeStr.empty()) {
        return std::string("");
    }

    const unsigned char *cur = reinterpret_cast<const unsigned char*>(toEncodeStr.c_str());

    // Three bytes to four bytes
    while (bytes > 2) {
        encodeRet += encodeTab_[cur[0] >> 2];
        encodeRet += encodeTab_[((cur[0] & 0x03) << 4) + (cur[1] >> 4)];
        encodeRet += encodeTab_[((cur[1] & 0x0f) << 2) + (cur[2] >> 6)];
        encodeRet += encodeTab_[cur[2] & 0x3f];
        cur += 3;
        bytes -= 3;
    }

    // Less than three bytes
    if (bytes > 0) {
        encodeRet += encodeTab_[cur[0] >> 2];
        if (bytes == 1) {
            encodeRet += encodeTab_[(cur[0] & 0x03) << 4];
            encodeRet += "==";
        } else if (bytes == 2) {
            encodeRet += encodeTab_[((cur[0] & 0x03) << 4) + (cur[1] >> 4)];
            encodeRet += encodeTab_[(cur[1] & 0x0f) << 2];
            encodeRet += "=";
        }
    }
    return encodeRet;
}

std::string Base64::decode(const std::string &toDecodeStr) {
    auto len = toDecodeStr.length();
    // The length of the string must be a multiple of four
    if ((len % 4) != 0) {
        return std::string("");
    }
    std::string decodeRet;
    int ret[4];
    int i = 0;

    int nLoop = toDecodeStr[len - 1]  == '=' ? len - 4 : len;
    for (i = 0; i < nLoop; i += 4) {
        ret[0] = decodeTab_[static_cast<int>(toDecodeStr[i])];
        ret[1] = decodeTab_[static_cast<int>(toDecodeStr[i+1])];
        ret[2] = decodeTab_[static_cast<int>(toDecodeStr[i+2])];
        ret[3] = decodeTab_[static_cast<int>(toDecodeStr[i+3])];

        if (ret[0] < 0 || ret[1] < 0 || ret[2] < 0 || ret[3] < 0) {
            return std::string("");
        }

        decodeRet += (ret[0] << 2) | ((ret[1] & 0x30) >> 4);
        decodeRet += ((ret[1] & 0xf) << 4) | ((ret[2] & 0x3c) >> 2);
        decodeRet += ((ret[2] & 0x3) << 6) | ret[3];
    }

    if (toDecodeStr[len - 1]  == '=' && toDecodeStr[len - 2] == '=') {
        ret[0] = decodeTab_[static_cast<int>(toDecodeStr[i])];
        ret[1] = decodeTab_[static_cast<int>(toDecodeStr[i+1])];
        if (ret[0] < 0 || ret[1] < 0) {
            return std::string("");
        }
        decodeRet += (ret[0] << 2) | ((ret[1] & 0x30) >> 4);
    }
    if (toDecodeStr[len - 1]  == '=' && toDecodeStr[len - 2] != '=') {
        ret[0] = decodeTab_[static_cast<int>(toDecodeStr[i])];
        ret[1] = decodeTab_[static_cast<int>(toDecodeStr[i+1])];
        ret[2] = decodeTab_[static_cast<int>(toDecodeStr[i+2])];
        if (ret[0] < 0 || ret[1] < 0 || ret[2] < 0) {
            return std::string("");
        }
        decodeRet += (ret[0] << 2) | ((ret[1] & 0x30) >> 4);
        decodeRet += ((ret[1] & 0xf) << 4) | ((ret[2] & 0x3c) >> 2);
    }

    return decodeRet;
}

}  // namespace encryption
}  // namespace nebula
