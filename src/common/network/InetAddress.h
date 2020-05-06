/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_NETWORK_INETADDRESS_H_
#define COMMON_NETWORK_INETADDRESS_H_

#include <folly/String.h>
#include <unistd.h>
#include <iostream>

#include <folly/SocketAddress.h>
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace network {
class InetAddress final {
public:
    InetAddress() : InetAddress(0, 0) {}
    explicit InetAddress(const std::string& host, uint16_t port, bool resolved = false)
        : addrs_{host, port, resolved} {
        if (resolved) {
            hostName_ = host;
        }
    }

    explicit InetAddress(uint32_t ip, uint16_t port, std::string hostName)
        : InetAddress(ip, port) {
        hostName_ = std::move(hostName);
    }

    // host byte order (Compatible with previous versions)
    InetAddress(uint32_t ip, uint16_t port = 0) : addrs_(folly::IPAddress::fromLongHBO(ip), port) {} // NOLINT

    explicit InetAddress(const nebula::cpp2::HostAddr& addr)
        : InetAddress(addr.get_ip(), addr.get_port()) {}

    explicit InetAddress(const nebula::cpp2::HostAddr* addr)
        : InetAddress(addr->get_ip(), addr->get_port()) {}

    InetAddress(InetAddress&&) = default;
    InetAddress(const InetAddress&) = default;

    // host byte order (Compatible with previous versions)
    static InetAddress makeInetAddress(const char* p);

    static InetAddress makeInetAddress(uint32_t ip, uint16_t port = 0, bool hbo = true);

    InetAddress& operator=(const InetAddress&) = default;

    bool operator==(const InetAddress& other) const {
        return addrs_ == other.addrs_;
    }

    bool operator!=(const InetAddress& other) const {
        return !(*this == other);
    }

    std::string getAddressStr() const {
        return addrs_.getAddressStr();
    }

    uint16_t getPort() const {
        return addrs_.getPort();
    }

    std::string getHostStr() const {
        return hostName_;
    }

    // for IPV4
    uint32_t toLong() const {
        return addrs_.getIPAddress().asV4().toLong();
    }

    uint32_t toLongHBO() const {
        return addrs_.getIPAddress().asV4().toLongHBO();
    }

    const folly::SocketAddress& getAddress() const {
        return addrs_;
    }

    std::string toString() const {
        return folly::stringPrintf("%s:%d", getAddressStr().c_str(), getPort());
    }

    bool isZero() const {
        return getPort() == 0 && toLong() == 0;
    }

    const std::string encode() const;

    bool isInitialized() const {
        return addrs_.isInitialized();
    }

    size_t hash() const {
        return addrs_.hash();
    }

private:
    folly::SocketAddress addrs_;
    std::string hostName_;
    explicit InetAddress(folly::SocketAddress addr) : addrs_(std::move(addr)) {}
};

std::ostream& operator<<(std::ostream&, const InetAddress&);
}   // namespace network
}   // namespace nebula
namespace std {
// Provide an implementation for std::hash<SocketAddress>
template <>
struct hash<nebula::network::InetAddress> {
    size_t operator()(const nebula::network::InetAddress& addr) const {
        return addr.hash();
    }
};
}   // namespace std
#endif
