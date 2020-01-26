/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "datatypes/HostAddr.h"

namespace nebula {

std::ostream& operator <<(std::ostream &os, const HostAddr &addr) {
    uint32_t ip = addr.ip;
    uint32_t port = addr.port;
    os << folly::stringPrintf("[%u.%u.%u.%u:%u]",
                              (ip >> 24) & 0xFF,
                              (ip >> 16) & 0xFF,
                              (ip >> 8) & 0xFF,
                              ip & 0xFF, port);
    return os;
}


bool HostAddr::operator==(const HostAddr& rhs) const {
    return ip == rhs.ip && port == rhs.port;
}

}  // namespace nebula

