/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "datatypes/HostAddr.h"

namespace nebula {

std::ostream& operator <<(std::ostream &os, const HostAddr &addr) {
    os << folly::stringPrintf("[%s:%u]", addr.host.c_str(), addr.port);
    return os;
}


bool HostAddr::operator==(const HostAddr& rhs) const {
    return host == rhs.host && port == rhs.port;
}

bool HostAddr::operator!=(const HostAddr& rhs) const {
    return !(*this == rhs);
}

bool HostAddr::operator<(const HostAddr& rhs) const {
    if (host == rhs.host) {
        return port < rhs.port;
    }
    return host < rhs.host;
}
}  // namespace nebula

