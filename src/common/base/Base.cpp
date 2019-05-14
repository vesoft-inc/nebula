/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

namespace nebula {

std::ostream& operator<<(std::ostream &os, const HostAddr &addr) {
    uint32_t ip = addr.first;
    uint32_t port = addr.second;
    os << folly::stringPrintf("[%u.%u.%u.%u:%u]",
                              (ip >> 24) & 0xFF,
                              (ip >> 16) & 0xFF,
                              (ip >> 8) & 0xFF,
                              ip & 0xFF, port);
    return os;
}

}   // namespace nebula
