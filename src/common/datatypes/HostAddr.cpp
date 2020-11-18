/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/hash/Hash.h>

#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"

namespace nebula {

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

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::HostAddr>::operator()(const nebula::HostAddr& h) const noexcept {
    int64_t code = folly::hash::fnv32_buf(h.host.data(), h.host.size());
    code <<= 32;
    code |= folly::hash::fnv32_buf(&(h.port), sizeof(nebula::Port));
    return code;
}

}  // namespace std
