/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_ENCRYPTION_MD5UTILS_H_
#define COMMON_ENCRYPTION_MD5UTILS_H_

#include <proxygen/lib/utils/CryptUtil.h>

namespace nebula {
namespace encryption {
class MD5Utils final {
public:
    MD5Utils() = delete;
    static std::string md5Encode(const std::string &str);
};
}  // namespace encryption
}  // namespace nebula
#endif  // COMMON_ENCRYPTION_MD5UTILS_H_
