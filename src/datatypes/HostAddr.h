/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
    std::string host;
    Port        port;

    HostAddr() : host(), port(0) {}
    HostAddr(std::string&& h, Port p) : host(std::move(h)), port(p) {}
    HostAddr(const std::string& h, Port p) : host(h), port(p) {}

    void clear() {
        host.clear();
        port = 0;
    }

    bool operator==(const HostAddr& rhs) const;

    bool operator!=(const HostAddr& rhs) const;

    bool operator<(const HostAddr& rhs) const;
};

std::ostream& operator<<(std::ostream &, const HostAddr&);

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::HostAddr> {
    std::size_t operator()(const nebula::HostAddr& h) const noexcept {
        int64_t code = folly::hash::fnv32_buf(&(h.host.data()), h.host.size());
        code <<= 32;
        code |= folly::hash::fnv32_buf(&(h.port), sizeof(nebula::Port));
        return code;
    }
};

}  // namespace std
#endif  // DATATYPES_HOSTADDR_H_
