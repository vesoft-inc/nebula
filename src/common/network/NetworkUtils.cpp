/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "network/NetworkUtils.h"
#include <netdb.h>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include "fs/FileUtils.h"

namespace nebula {
namespace network {

StatusOr<std::string> NetworkUtils::getIPv4FromDevice(const std::string &device) {
    if (device == "any") {
        return "0.0.0.0";
    }
    auto result = listDeviceAndIPv4s();
    if (!result.ok()) {
        return std::move(result).status();
    }
    auto iter = result.value().find(device);
    if (iter == result.value().end()) {
        return Status::Error("No IPv4 address found for `%s'", device.c_str());
    }
    return iter->second;
}


StatusOr<std::vector<std::string>> NetworkUtils::listIPv4s() {
    auto result = listDeviceAndIPv4s();
    if (!result.ok()) {
        return std::move(result).status();
    }
    auto getval = [] (const auto &entry) {
        return entry.second;
    };
    std::vector<std::string> ipv4s;
    ipv4s.resize(result.value().size());
    std::transform(result.value().begin(), result.value().end(), ipv4s.begin(), getval);
    return ipv4s;
}


StatusOr<std::unordered_map<std::string, std::string>> NetworkUtils::listDeviceAndIPv4s() {
    struct ifaddrs *iflist;
    std::unordered_map<std::string, std::string> dev2ipv4s;
    if (::getifaddrs(&iflist) != 0) {
        return Status::Error("%s", ::strerror(errno));
    }
    for (auto *ifa = iflist; ifa != nullptr; ifa = ifa->ifa_next) {
        // Skip non-IPv4 devices
        if (ifa->ifa_addr->sa_family != AF_INET) {
            continue;
        }
        auto *addr = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
        // inet_ntoa is thread safe but not re-entrant,
        // we could use inet_ntop instead when we need support for IPv6
        dev2ipv4s[ifa->ifa_name] = ::inet_ntoa(addr->sin_addr);
    }
    ::freeifaddrs(iflist);
    if (dev2ipv4s.empty()) {
        return Status::Error("No IPv4 devices found");
    }
    return dev2ipv4s;
}


bool NetworkUtils::getDynamicPortRange(uint16_t& low, uint16_t& high) {
    FILE* pipe = popen("cat /proc/sys/net/ipv4/ip_local_port_range", "r");
    if (!pipe) {
        LOG(ERROR) << "Failed to open /proc/sys/net/ipv4/ip_local_port_range: "
                   << strerror(errno);
        return false;
    }

    if (fscanf(pipe, "%hu %hu", &low, &high) != 2) {
        LOG(ERROR) << "Failed to read from /proc/sys/net/ipv4/ip_local_port_range";
        // According to ICANN, the port range is devided into three sections
        //
        // Well-known ports: 0 to 1023 (used for system services)
        // Registered/user ports: 1024 to 49151
        // Dynamic/private ports: 49152 to 65535
        low = 49152;
        high = 65535;
    }

    if (pclose(pipe) < 0) {
        LOG(ERROR) << "Failed to close the pipe: " << strerror(errno);
        return false;
    }

    return true;
}


std::unordered_set<uint16_t> NetworkUtils::getPortsInUse() {
    static const std::regex regex("[^:]+:[^:]+:([0-9A-F]+).+");
    std::unordered_set<uint16_t> inUse;
    {
        fs::FileUtils::FileLineIterator iter("/proc/net/tcp", &regex);
        while (iter.valid()) {
            auto &sm = iter.matched();
            inUse.emplace(std::stoul(sm[1].str(), NULL, 16));
            ++iter;
        }
    }
    {
        fs::FileUtils::FileLineIterator iter("/proc/net/tcp6", &regex);
        while (iter.valid()) {
            auto &sm = iter.matched();
            inUse.emplace(std::stoul(sm[1].str(), NULL, 16));
            ++iter;
        }
    }
    {
        fs::FileUtils::FileLineIterator iter("/proc/net/udp", &regex);
        while (iter.valid()) {
            auto &sm = iter.matched();
            inUse.emplace(std::stoul(sm[1].str(), NULL, 16));
            ++iter;
        }
    }
    {
        fs::FileUtils::FileLineIterator iter("/proc/net/udp6", &regex);
        while (iter.valid()) {
            auto &sm = iter.matched();
            inUse.emplace(std::stoul(sm[1].str(), NULL, 16));
            ++iter;
        }
    }
    {
        fs::FileUtils::FileLineIterator iter("/proc/net/raw", &regex);
        while (iter.valid()) {
            auto &sm = iter.matched();
            inUse.emplace(std::stoul(sm[1].str(), NULL, 16));
            ++iter;
        }
    }
    {
        fs::FileUtils::FileLineIterator iter("/proc/net/raw6", &regex);
        while (iter.valid()) {
            auto &sm = iter.matched();
            inUse.emplace(std::stoul(sm[1].str(), NULL, 16));
            ++iter;
        }
    }

    return inUse;
}


uint16_t NetworkUtils::getAvailablePort() {
    uint16_t low = 0;
    uint16_t high = 0;

    CHECK(getDynamicPortRange(low, high))
        << "Failed to get the dynamic port range";
    VLOG(1) << "Dynamic port range is [" << low << ", " << high << "]";

    std::unordered_set<uint16_t> portsInUse = getPortsInUse();
    uint16_t port = 0;
    while (true) {
        // NOTE
        // The availablity of port number *outside* the ephemeral port range is
        // relatively stable for the binding purpose.
        port = folly::Random::rand32(1025, low);
        if (portsInUse.find(port) != portsInUse.end()) {
            continue;
        }
        if (portsInUse.find(port + 1) == portsInUse.end()) {
            break;
        }
    }

    return port;
}


bool NetworkUtils::ipv4ToInt(const std::string& ipStr, IPv4& ip) {
    try {
        folly::IPAddress addr(ipStr);
        ip = addr.asV4().toLong();
        return true;
    } catch (std::exception& e) {
        LOG(ERROR) << "Invalid ip string: \"" << ipStr << "\", err: %s", e.what();
        return false;
    }
}

std::string NetworkUtils::intToIPv4(IPv4 ip) {
    return folly::IPAddress::fromLong(ip).str();
}

StatusOr<InetAddress> NetworkUtils::toInetAddress(const std::string& ip, uint16_t port) {
    try {
        return InetAddress{ip, port};
    } catch (const std::exception& e) {
        return Status::Error("Bad ip format: %s", e.what());
    }
}

StatusOr<std::vector<InetAddress>> NetworkUtils::toHosts(const std::string& peersStr) {
    std::vector<InetAddress> hosts;
    std::vector<std::string> peers;
    folly::split(",", peersStr, peers, true);
    hosts.reserve(peers.size());
    for (auto& peerStr : peers) {
        auto ipPort = folly::trimWhitespace(peerStr);
        auto pos = ipPort.find(':');
        if (pos == folly::StringPiece::npos) {
            return Status::Error("Bad peer format: %s", ipPort.start());
        }

        uint16_t port;
        try {
            port = folly::to<uint16_t>(ipPort.subpiece(pos + 1));
        } catch (const std::exception& ex) {
            return Status::Error("Bad port number, error: %s", ex.what());
        }

        auto ipStr = ipPort.subpiece(0, pos).toString();

        try {
            hosts.emplace_back(ipStr, port);
        } catch (const std::exception& e) {
            try {
                LOG(INFO) << "will resolve host: " << ipStr;
                hosts.emplace_back(ipStr, port, true);
            } catch (const std::exception& e) {
                return Status::Error("Bad ip format: %s", e.what());
            }
        }
    }
    return hosts;
}

std::string NetworkUtils::toHostsString(const std::vector<InetAddress>& hosts) {
    std::string hostsString = "";
    for (auto& host : hosts) {
        hostsString += folly::stringPrintf("%s, ", host.toString().c_str());
    }
    if (!hostsString.empty()) {
        hostsString.resize(hostsString.size() - 2);
    }
    return hostsString;
}

std::string NetworkUtils::ipFromInetAddress(const InetAddress& host) {
    return host.getAddressStr();
}

uint16_t NetworkUtils::portFromInetAddress(const InetAddress& host) {
    return host.getPort();
}

StatusOr<std::string> NetworkUtils::getLocalIP(std::string defaultIP) {
    if (!defaultIP.empty()) {
        return defaultIP;
    }
    auto result = network::NetworkUtils:: listDeviceAndIPv4s();
    if (!result.ok()) {
        return std::move(result).status();
    }
    for (auto& deviceIP : result.value()) {
        if (deviceIP.second != "127.0.0.1") {
            return deviceIP.second;
        }
    }
    return Status::Error("No IPv4 address found!");
}

}  // namespace network
}  // namespace nebula
