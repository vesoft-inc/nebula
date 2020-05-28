/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_NETWORK_NETWORKUTILS_H_
#define COMMON_NETWORK_NETWORKUTILS_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/HostAddr.h"

namespace nebula {
namespace network {

class NetworkUtils final {
public:
    NetworkUtils() = delete;

    static std::string getHostname();

    // Get the Ipv4 address bound to a specific net device.
    // If given "any", it returns "0.0.0.0".
    static StatusOr<std::string> getIPv4FromDevice(const std::string &device);
    // List out all Ipv4 addresses, including the loopback one.
    static StatusOr<std::vector<std::string>> listIPv4s();
    // List out all network devices and its cooresponding Ipv4 address.
    static StatusOr<std::unordered_map<std::string, std::string>> listDeviceAndIPv4s();

    // Get the local dynamic port range [low, high], only works for IPv4
    static bool getDynamicPortRange(uint16_t& low, uint16_t& high);
    // Get all ports that are currently in use
    static std::unordered_set<uint16_t> getPortsInUse();
    // To get a port number which is available to bind on.
    // The availability is not guaranteed, e.g. in the parallel case.
    //
    // Note that this function is to be used for testing purpose.
    // So don't use it in production code.
    static uint16_t getAvailablePort();

    static StatusOr<std::vector<HostAddr>> resolveHost(const std::string &host,
                                                       int32_t port);

    // Convert the given 32-bit unsigned integer (in network order) to an IPv4
    // address string (in the form of xx.xx.xx.xx)
    static std::string intToIPv4(uint32_t ip);

    // Convert peers str which is a list of ipPort joined with comma into HostAddr list.
    // (Peers str format example: 192.168.1.1:10001, 192.168.1.2:10001)
    // Return Status::Error if peersStr is invalid.
    static StatusOr<std::vector<HostAddr>> toHosts(const std::string& peersStr);
    static std::string toHostsStr(const std::vector<HostAddr>& hosts);

private:
};

}  // namespace network
}  // namespace nebula

#endif  // COMMON_NETWORK_NETWORKUTILS_H_
