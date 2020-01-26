/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_HOSTADDR_H_
#define DATATYPES_HOSTADDR_H_

#include "base/Base.h"
#include <folly/hash/Hash.h>
#include "thrift/ThriftTypes.h"

namespace nebula {

// Host address type and utility functions
struct HostAddr {
    IPv4 ip;
    Port port;

    HostAddr() : ip(0), port(0) {}
    HostAddr(IPv4 a, Port p) : ip(a), port(p) {}

    bool operator==(const HostAddr& rhs) const;
};

std::ostream& operator<<(std::ostream &, const HostAddr&);

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::HostAddr> {
    std::size_t operator()(const nebula::HostAddr& h) const noexcept {
        return folly::hash::fnv64_buf(&h, sizeof(nebula::HostAddr));
    }
};

}  // namespace std
#endif  // DATATYPES_HOSTADDR_H_
