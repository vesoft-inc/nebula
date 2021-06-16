/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_HOSTADDR_H_
#define COMMON_DATATYPES_HOSTADDR_H_

#include <sstream>

#include "common/thrift/ThriftTypes.h"

namespace nebula {

// Host address type and utility functions
struct HostAddr {
    std::string host;
    Port        port;

    HostAddr() : host(), port(0) {}
    /*
     * some one may ctor HostAddr this way : HostAddr host(0, 0)
     * C++ will compile this successfully even we don't support an (int, int) ctor
     * so, add an explicit delete ctor
     * */
    HostAddr(int h, int p) = delete;
    HostAddr(std::string h, Port p) : host(std::move(h)), port(p) {}

    void clear() {
        host.clear();
        port = 0;
    }

    void __clear() {
        clear();
    }

    std::string toString() const {
        std::stringstream os;
        os << "\"" << host << "\"" << ":" << port;
        return os.str();
    }

    bool operator==(const HostAddr& rhs) const;

    bool operator!=(const HostAddr& rhs) const;

    bool operator<(const HostAddr& rhs) const;
};

inline std::ostream& operator <<(std::ostream& os, const HostAddr& addr) {
    return os << addr.toString();
}

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::HostAddr> {
    std::size_t operator()(const nebula::HostAddr& h) const noexcept;
};

}  // namespace std

#endif  // COMMON_DATATYPES_HOSTADDR_H_
